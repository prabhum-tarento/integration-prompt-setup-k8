using System.Text;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Confluent.Kafka;
using IIS.WMS.Consumer.Infrastructure.Resilience;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly.Registry;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Generic Kafka → Service Bus relay (integration-resiliency.instructions.md §1) shared by every
/// consumer regardless of wire format - <c>TValue</c> is already the deserialized message (JSON or
/// Avro; the deserializer is supplied by the derived class's constructor, wired into the
/// <see cref="IConsumer{TKey,TValue}"/> it builds). A derived class only needs to say how one
/// <c>TValue</c> maps onto a Service Bus session id, deterministic message id, and body
/// (<see cref="MapToServiceBusMessage"/>) - everything else (poll loop, bounded-channel worker pool,
/// poison-message handling, ordered offset commit, resilience, correlation id propagation, the
/// <see cref="ConsumerOptions.Enabled"/> toggle) lives here once.
/// </summary>
/// <remarks>
/// The single-threaded poll loop reads from Kafka and writes each result into a bounded
/// <see cref="Channel{T}"/>; <see cref="ConsumerOptions.WorkerCount"/> concurrent workers drain it
/// and publish to Service Bus (integration-resiliency.instructions.md §6). Because workers can
/// finish out of order, offset commits go through <see cref="PartitionOffsetCommitTracker"/>, which
/// only advances (and commits) a partition's low-water mark once every offset below it has
/// completed - never a plain <c>consumer.Commit(result)</c> per message. This does not handle a
/// partition being revoked and reassigned mid-flight (no
/// <c>SetPartitionsRevokedHandler</c>/<c>SetPartitionsAssignedHandler</c> yet) - a rebalance while
/// messages are in flight can produce a duplicate delivery to whichever consumer picks the
/// partition up next, which the existing downstream dedupe (§2) already has to tolerate for other
/// reasons, but it's called out here as a known gap rather than something this design solves.
/// </remarks>
/// <typeparam name="TValue">The deserialized Kafka message value type this consumer relays.</typeparam>
public abstract class ConsumerHostedService<TValue> : BackgroundService
{
    private readonly IConsumer<string, TValue> consumer;
    private readonly IDisposable? additionalDisposable;
    private readonly ServiceBusSender serviceBusSender;
    private readonly ResiliencePipelineProvider<string> pipelineProvider;
    private readonly ConsumerHealthState healthState;
    private readonly ILogger logger;
    private readonly PartitionOffsetCommitTracker offsetTracker;

    /// <summary>Settings this consumer was configured with.</summary>
    protected ConsumerOptions Options { get; }

    /// <summary>Display name used in log messages - distinguishes this consumer's log lines from other consumers sharing the same pod/process.</summary>
    protected string ConsumerName { get; }

    /// <summary>Builds the Kafka consumer (with the supplied deserializer already wired in) and the Service Bus sender it relays onto.</summary>
    /// <param name="options">Topic, consumer group, enabled flag, worker/channel sizing, and Service Bus queue settings for this consumer.</param>
    /// <param name="consumerName">Display name for this consumer's log messages.</param>
    /// <param name="valueDeserializer">Deserializer for the Kafka message value - JSON or Avro, supplied by the derived class.</param>
    /// <param name="serviceBusClient">Client used to create the sender for the relay queue.</param>
    /// <param name="pipelineProvider">Resolves the named Polly pipeline used for the Service Bus publish step.</param>
    /// <param name="healthState">Shared state updated on every poll, read by this consumer's <see cref="ConsumerHealthCheck"/>.</param>
    /// <param name="logger">Logger for consume/relay/poison-message events.</param>
    /// <param name="additionalDisposable">An extra resource the derived class owns (e.g. an <c>ISchemaRegistryClient</c>) and wants disposed alongside the consumer - <see langword="null"/> if none.</param>
    protected ConsumerHostedService(
        ConsumerOptions options,
        string consumerName,
        IDeserializer<TValue> valueDeserializer,
        ServiceBusClient serviceBusClient,
        ResiliencePipelineProvider<string> pipelineProvider,
        ConsumerHealthState healthState,
        ILogger logger,
        IDisposable? additionalDisposable = null)
    {
        Options = options;
        ConsumerName = consumerName;
        this.pipelineProvider = pipelineProvider;
        this.healthState = healthState;
        this.logger = logger;
        this.additionalDisposable = additionalDisposable;

        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.ConsumerGroup,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        consumer = new ConsumerBuilder<string, TValue>(config)
            .SetValueDeserializer(valueDeserializer)
            .Build();

        serviceBusSender = serviceBusClient.CreateSender(options.ServiceBusQueueName);
        offsetTracker = new PartitionOffsetCommitTracker(offsets => consumer.Commit(offsets));
    }

    /// <summary>Maps one deserialized message onto the Service Bus wire shape.</summary>
    /// <param name="value">The deserialized Kafka message value.</param>
    /// <returns>The Service Bus session id (groups this event with others for the same aggregate), a deterministic message id (drives the downstream dedupe check), and the serialized body to send.</returns>
    protected abstract (string SessionId, string MessageId, string Body) MapToServiceBusMessage(TValue value);

    /// <summary>
    /// Polls the subscribed topic in a loop until cancellation, handing each message to the worker
    /// pool for relaying - a no-op if <see cref="ConsumerOptions.Enabled"/> is <see langword="false"/>.
    /// </summary>
    /// <param name="stoppingToken">Signaled when the host is shutting down.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!Options.Enabled)
        {
            logger.LogInformation("{ConsumerName} is disabled via configuration ('Enabled: false') - not starting.", ConsumerName);
            return;
        }

        consumer.Subscribe(Options.Topic);

        var channel = Channel.CreateBounded<ConsumeResult<string, TValue>>(new BoundedChannelOptions(Options.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true,
        });

        // Linked so a worker fault (e.g. Polly retries exhausted on a publish) stops the poll loop
        // promptly instead of continuing to buffer messages behind a partition that can no longer
        // make progress - see the class-level remarks.
        using var shutdownCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        var workers = Enumerable.Range(0, Options.WorkerCount)
            .Select(_ => RunWorkerAsync(channel.Reader, shutdownCts))
            .ToArray();

        try
        {
            await RunPollLoopAsync(channel.Writer, shutdownCts.Token);
        }
        finally
        {
            channel.Writer.TryComplete();

            // Awaited even after a worker fault, so every worker's ReadAllAsync loop above actually
            // observes the completed channel and exits - if we returned immediately instead, those
            // tasks would be silently abandoned still holding a channel reader.
            await Task.WhenAll(workers);
        }
    }

    /// <summary>Single-threaded poll loop - reads from Kafka and hands each result to the worker pool via the channel, applying backpressure when it's full.</summary>
    private async Task RunPollLoopAsync(ChannelWriter<ConsumeResult<string, TValue>> writer, CancellationToken shutdownToken)
    {
        while (!shutdownToken.IsCancellationRequested)
        {
            ConsumeResult<string, TValue>? polled;

            try
            {
                polled = consumer.Consume(Options.PollTimeout);
            }
            catch (ConsumeException ex)
            {
                var consumerRecord = ex.ConsumerRecord;

                if (consumerRecord is null)
                {
                    logger.LogError(ex, "{ConsumerName}: Kafka consume error on topic {Topic}.", ConsumerName, Options.Topic);
                    continue;
                }

                // Poison message: deserialization failed inside Consume() itself, so it never
                // reaches the worker pool's publish step. Route it through the same offset tracker
                // as a normal completion - marking it done, not committing directly - so it folds
                // correctly into whatever the partition's low-water mark already is
                // (integration-resiliency.instructions.md §1).
                var topicPartitionOffset = consumerRecord.TopicPartitionOffset;
                var rawValue = consumerRecord.Message?.Value is { } bytes ? Encoding.UTF8.GetString(bytes) : "<null>";

                logger.LogCritical(ex,
                    "{ConsumerName}: poison message at {Topic}:{Partition}:{Offset} - could not deserialize. Raw payload: {RawPayload}",
                    ConsumerName, topicPartitionOffset.Topic, topicPartitionOffset.Partition.Value, topicPartitionOffset.Offset.Value, rawValue);

                offsetTracker.EstablishBaseline(topicPartitionOffset.TopicPartition, topicPartitionOffset.Offset.Value);
                offsetTracker.Complete(topicPartitionOffset);

                continue;
            }

            // No message within the poll timeout is not a failure - an idle topic keeps the
            // consumer healthy (integration-resiliency.instructions.md §8).
            healthState.LastSuccessfulPollUtc = DateTimeOffset.UtcNow;

            if (polled is not { Message: not null } result)
            {
                continue;
            }

            // Established here, in the single-threaded poll loop, before the message reaches any
            // worker - see PartitionOffsetCommitTracker.EstablishBaseline for why that ordering is
            // what makes it correct.
            offsetTracker.EstablishBaseline(result.TopicPartition, result.Offset.Value);

            await writer.WriteAsync(result, shutdownToken);
        }
    }

    /// <summary>One worker draining the channel - relays each message to Service Bus and reports the outcome to the offset tracker, until the channel completes.</summary>
    private async Task RunWorkerAsync(ChannelReader<ConsumeResult<string, TValue>> reader, CancellationTokenSource shutdownCts)
    {
        try
        {
            // Intentionally CancellationToken.None, not the shutdown token: once a message has been
            // dispatched into the channel it should still be relayed and committed on a graceful
            // shutdown, not abandoned mid-flight only to be redelivered on restart.
            await foreach (var result in reader.ReadAllAsync(CancellationToken.None))
            {
                await ProcessMessageAsync(result, CancellationToken.None);
            }
        }
        catch (Exception ex)
        {
            // A publish that exhausted the resilience pipeline's retries is not recoverable by this
            // process - stop this consumer so Kubernetes restarts it and Kafka redelivers from the
            // last committed offset, the same fail-fast behavior the original single-threaded loop
            // had when an unhandled exception escaped it.
            logger.LogCritical(ex, "{ConsumerName}: worker faulted relaying a message - stopping this consumer.", ConsumerName);
            shutdownCts.Cancel();
            throw;
        }
    }

    /// <summary>Relays one consumed message to Service Bus and reports the outcome to the offset tracker.</summary>
    /// <param name="result">The consumed Kafka message, with its topic/partition/offset metadata.</param>
    /// <param name="cancellationToken">Token to cancel the publish.</param>
    private async Task ProcessMessageAsync(ConsumeResult<string, TValue> result, CancellationToken cancellationToken)
    {
        var (sessionId, messageId, body) = MapToServiceBusMessage(result.Message.Value);
        var correlationId = TryGetCorrelationIdHeader(result.Message.Headers) ?? Guid.NewGuid().ToString();

        var message = new ServiceBusMessage(body)
        {
            // Deterministic id from the event payload - this is what makes the Service Bus
            // consumer's dedupe check on redelivery actually work.
            MessageId = messageId,
            SessionId = sessionId,
        };
        message.ApplicationProperties["CorrelationId"] = correlationId;

        var pipeline = pipelineProvider.GetPipeline(ResiliencePipelines.ServiceBusPublish);

        await pipeline.ExecuteAsync(
            async ct => await serviceBusSender.SendMessageAsync(message, ct), cancellationToken);

        // Reports completion to the tracker rather than committing this offset directly - with
        // multiple workers in flight, this message's offset is not necessarily the next one the
        // partition is waiting on (integration-resiliency.instructions.md §6).
        offsetTracker.Complete(result.TopicPartitionOffset);

        logger.LogInformation(
            "{ConsumerName}: relayed {MessageId} for session {SessionId} from {Topic}:{Partition}:{Offset} to Service Bus. CorrelationId: {CorrelationId}",
            ConsumerName, messageId, sessionId, result.Topic, result.Partition.Value, result.Offset.Value, correlationId);
    }

    /// <summary>Reads the <c>correlationId</c> Kafka header, if present (integration-resiliency.instructions.md §4).</summary>
    /// <param name="headers">Headers on the consumed Kafka message, or <see langword="null"/> if none were sent.</param>
    /// <returns>The header value, or <see langword="null"/> if absent.</returns>
    protected static string? TryGetCorrelationIdHeader(Headers? headers)
    {
        return headers is not null && headers.TryGetLastBytes("correlationId", out var bytes)
            ? Encoding.UTF8.GetString(bytes)
            : null;
    }

    /// <summary>Closes and disposes the underlying Kafka consumer, plus any derived-class resource passed as <c>additionalDisposable</c> - Confluent.Kafka has no async-dispose equivalent.</summary>
    public override void Dispose()
    {
        consumer.Close();
        consumer.Dispose();
        additionalDisposable?.Dispose();
        base.Dispose();
    }
}
