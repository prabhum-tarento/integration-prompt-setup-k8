using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Confluent.Kafka;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Infrastructure.BlobStorage;
using IIS.WMS.Consumer.Infrastructure.Resilience;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly.Registry;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Generic Kafka → Service Bus relay (integration-resiliency.instructions.md §1) shared by every
/// consumer regardless of wire format - <c>TValue</c> is the deserialized message shape (JSON or
/// Avro), but deserialization itself now happens manually, after the dedup check below, not inside
/// <see cref="IConsumer{TKey,TValue}.Consume(TimeSpan)"/> - see the class-level flow remarks. A
/// derived class only needs to say how one <c>TValue</c> maps onto a Service Bus session id,
/// deterministic message id, and body (<see cref="MapToServiceBusMessage"/>) - everything else (poll
/// loop, bounded-channel worker pool, cold/hot-tier audit logging, deduplication, poison-message
/// handling, ordered offset commit, resilience, correlation id propagation, the
/// <see cref="ConsumerOptions.Enabled"/> toggle) lives here once.
/// </summary>
/// <remarks>
/// <para>
/// Per-message flow: consume raw bytes → read Kafka headers (correlation id, dedup id, event
/// type, app id) → log them → write the raw header+body to the cold-tier audit container
/// (<see cref="BlobStorageOptions.RequestAuditContainerName"/>, unconditionally - not gated by
/// <see cref="BlobStorageOptions.RequestAuditEnabled"/>, which covers a separate, still-optional
/// audit use) → check <see cref="IDeduplicationService"/> → deserialize → map and publish to Service
/// Bus → commit. A deserialization failure is a poison message: its raw header+body goes to the
/// hot-tier <see cref="BlobStorageOptions.ConsumerDeadLetterContainerName"/> container and the offset
/// is committed forward (never redelivered). An unrecoverable Service Bus publish failure (Polly
/// retries in <see cref="ResiliencePipelines.ServiceBusPublish"/> exhausted) is deliberately handled
/// differently - it is NOT written to hot storage, it faults the worker and stops this consumer so
/// Kubernetes restarts the pod and Kafka redelivers, exactly as before this change - a sustained
/// Service Bus outage should surface as an outage, not silently drain the topic into blob storage.
/// </para>
/// <para>
/// The single-threaded poll loop reads from Kafka and writes each result into a bounded
/// <see cref="Channel{T}"/>; <see cref="ConsumerOptions.WorkerCount"/> concurrent workers drain it
/// and run the flow above (integration-resiliency.instructions.md §6). Because workers can finish out
/// of order, offset commits go through <see cref="PartitionOffsetCommitTracker"/>, which only
/// advances (and commits) a partition's low-water mark once every offset below it has completed -
/// never a plain <c>consumer.Commit(result)</c> per message. This does not handle a partition being
/// revoked and reassigned mid-flight (no
/// <c>SetPartitionsRevokedHandler</c>/<c>SetPartitionsAssignedHandler</c> yet) - a rebalance while
/// messages are in flight can produce a duplicate delivery to whichever consumer picks the partition
/// up next, which the dedup check above now also covers, in addition to the existing downstream
/// Service Bus consumer dedupe (§2).
/// </para>
/// </remarks>
/// <typeparam name="TValue">The deserialized Kafka message value type this consumer relays.</typeparam>
public abstract class ConsumerHostedService<TValue> : BackgroundService
{
    // Every consumer's schema/message type name - the third path segment of the cold/hot-tier blob
    // path convention below.
    private static readonly string SchemaName = typeof(TValue).Name;

    private readonly IConsumer<string, byte[]> consumer;
    private readonly IDeserializer<TValue> valueDeserializer;
    private readonly IDisposable? additionalDisposable;
    private readonly ServiceBusSender serviceBusSender;
    private readonly ResiliencePipelineProvider<string> pipelineProvider;
    private readonly IFileStore fileStore;
    private readonly IDeduplicationService deduplicationService;
    private readonly ConsumerHealthState healthState;
    private readonly ILogger logger;
    private readonly PartitionOffsetCommitTracker offsetTracker;

    /// <summary>Settings this consumer was configured with.</summary>
    protected ConsumerOptions Options { get; }

    /// <summary>Display name used in log messages and the cold/hot-tier blob path - distinguishes this consumer's log lines and audit records from other consumers sharing the same pod/process.</summary>
    protected string ConsumerName { get; }

    /// <summary>Builds the Kafka consumer and the Service Bus sender it relays onto.</summary>
    /// <param name="options">Topic, consumer group, enabled flag, worker/channel sizing, and Service Bus queue settings for this consumer.</param>
    /// <param name="consumerName">Display name for this consumer's log messages and audit blob path.</param>
    /// <param name="valueDeserializer">Deserializer for the Kafka message value (JSON or Avro), supplied by the derived class - invoked manually, after the dedup check, not wired into the Kafka consumer itself.</param>
    /// <param name="serviceBusClient">Client used to create the sender for the relay queue.</param>
    /// <param name="pipelineProvider">Resolves the named Polly pipeline used for the Service Bus publish step.</param>
    /// <param name="fileStore">Writes the cold-tier (every message) and hot-tier (deserialization failures) audit blobs.</param>
    /// <param name="deduplicationService">Checks each message against the Nexus deduplication service before it is deserialized.</param>
    /// <param name="healthState">Shared state updated on every poll, read by this consumer's <see cref="ConsumerHealthCheck"/>.</param>
    /// <param name="logger">Logger for consume/relay/poison-message events.</param>
    /// <param name="additionalDisposable">An extra resource the derived class owns (e.g. an <c>ISchemaRegistryClient</c>) and wants disposed alongside the consumer - <see langword="null"/> if none.</param>
    protected ConsumerHostedService(
        ConsumerOptions options,
        string consumerName,
        IDeserializer<TValue> valueDeserializer,
        ServiceBusClient serviceBusClient,
        ResiliencePipelineProvider<string> pipelineProvider,
        IFileStore fileStore,
        IDeduplicationService deduplicationService,
        ConsumerHealthState healthState,
        ILogger logger,
        IDisposable? additionalDisposable = null)
    {
        Options = options;
        ConsumerName = consumerName;
        this.valueDeserializer = valueDeserializer;
        this.pipelineProvider = pipelineProvider;
        this.fileStore = fileStore;
        this.deduplicationService = deduplicationService;
        this.healthState = healthState;
        this.logger = logger;
        this.additionalDisposable = additionalDisposable;

        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers
                ?? throw new InvalidOperationException(
                    $"Missing BootstrapServers for '{consumerName}' - configure it at this consumer's own level or the Kafka-level fallback."),
            GroupId = options.ConsumerGroup,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        // Raw bytes, not TValue - Confluent.Kafka's built-in default byte[] deserializer never
        // throws, so a bad payload is only ever discovered later, in ProcessMessageAsync, after the
        // cold-tier audit log and dedup check have already run against the raw bytes. See the
        // class-level remarks for why this moved out of the consumer builder.
        consumer = new ConsumerBuilder<string, byte[]>(config).Build();

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
        if (Options.Enabled != true)
        {
            logger.LogInformation("{ConsumerName} is disabled via configuration ('Enabled: false') - not starting.", ConsumerName);
            return;
        }

        consumer.Subscribe(Options.Topic);

        var channel = Channel.CreateBounded<ConsumeResult<string, byte[]>>(new BoundedChannelOptions(Options.ChannelCapacity)
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

    /// <summary>Single-threaded poll loop - reads raw messages from Kafka and hands each result to the worker pool via the channel, applying backpressure when it's full.</summary>
    private async Task RunPollLoopAsync(ChannelWriter<ConsumeResult<string, byte[]>> writer, CancellationToken shutdownToken)
    {
        while (!shutdownToken.IsCancellationRequested)
        {
            ConsumeResult<string, byte[]>? polled;

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

                // A genuine Kafka-level consume failure (e.g. the key failed to deserialize as
                // UTF-8) - the message value's own deserialization can no longer fail here since the
                // consumer only ever reads raw bytes now (see the constructor remarks); that failure
                // mode is handled in ProcessMessageAsync instead. Still routed through the offset
                // tracker as a normal completion - marking it done, not committing directly - so it
                // folds correctly into whatever the partition's low-water mark already is
                // (integration-resiliency.instructions.md §1).
                var topicPartitionOffset = consumerRecord.TopicPartitionOffset;
                var rawValue = consumerRecord.Message?.Value is { } bytes ? Encoding.UTF8.GetString(bytes) : "<null>";

                logger.LogCritical(ex,
                    "{ConsumerName}: Kafka-level consume error at {Topic}:{Partition}:{Offset} - message skipped. Raw payload: {RawPayload}",
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

    /// <summary>One worker draining the channel - runs the full per-message flow and reports the outcome to the offset tracker, until the channel completes.</summary>
    private async Task RunWorkerAsync(ChannelReader<ConsumeResult<string, byte[]>> reader, CancellationTokenSource shutdownCts)
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
            // had when an unhandled exception escaped it. Deliberately not the same handling as a
            // deserialization failure (see ProcessMessageAsync) - see the class-level remarks.
            logger.LogCritical(ex, "{ConsumerName}: worker faulted relaying a message - stopping this consumer.", ConsumerName);
            shutdownCts.Cancel();
            throw;
        }
    }

    /// <summary>
    /// Runs the full per-message flow: log metadata, write the cold-tier audit log, check
    /// deduplication, deserialize, map and publish to Service Bus, then report completion to the
    /// offset tracker. See the class-level remarks for the exact step order and failure handling.
    /// </summary>
    /// <param name="result">The consumed raw Kafka message, with its topic/partition/offset metadata.</param>
    /// <param name="cancellationToken">Token to cancel the publish.</param>
    private async Task ProcessMessageAsync(ConsumeResult<string, byte[]> result, CancellationToken cancellationToken)
    {
        var headers = result.Message.Headers;
        var correlationId = TryGetHeader(headers, KafkaHeaderNames.CorrelationId) ?? Guid.NewGuid().ToString();
        var deduplicationId = TryGetHeader(headers, KafkaHeaderNames.DeduplicationId) ?? string.Empty;
        var eventType = TryGetHeader(headers, KafkaHeaderNames.Type) ?? string.Empty;
        var appId = TryGetHeader(headers, KafkaHeaderNames.AppId) ?? string.Empty;

        logger.LogInformation(
            "{ConsumerName}: consumed message from {Topic}:{Partition}:{Offset}. CorrelationId: {CorrelationId}, EventType: {EventType}, AppId: {AppId}",
            ConsumerName, result.Topic, result.Partition.Value, result.Offset.Value, correlationId, eventType, appId);

        await WriteAuditLogAsync(
            BlobStorageOptions.RequestAuditContainerName, "cold", correlationId, result, exception: null, cancellationToken);

        if (await deduplicationService.IsDuplicateAsync(ConsumerName, deduplicationId, correlationId, cancellationToken))
        {
            logger.LogInformation(
                "{ConsumerName}: skipping {Topic}:{Partition}:{Offset} - duplicate. CorrelationId: {CorrelationId}",
                ConsumerName, result.Topic, result.Partition.Value, result.Offset.Value, correlationId);

            offsetTracker.Complete(result.TopicPartitionOffset);
            return;
        }

        TValue value;

        try
        {
            var rawValue = result.Message.Value;
            value = valueDeserializer.Deserialize(
                rawValue, rawValue is null, new SerializationContext(MessageComponentType.Value, result.Topic, headers));
        }
        catch (Exception ex)
        {
            // Poison message: never reaches the publish step, so the offset-after-publish rule would
            // otherwise replay it forever and stall every message behind it on this partition. Its
            // raw header+body goes to the hot-tier container for manual recovery, then the offset is
            // committed forward - not a direct consumer.Commit, so it folds correctly into whatever
            // the partition's low-water mark already is (integration-resiliency.instructions.md §1).
            logger.LogCritical(ex,
                "{ConsumerName}: failed to deserialize message at {Topic}:{Partition}:{Offset} - writing to hot storage and committing. CorrelationId: {CorrelationId}",
                ConsumerName, result.Topic, result.Partition.Value, result.Offset.Value, correlationId);

            await WriteAuditLogAsync(
                BlobStorageOptions.ConsumerDeadLetterContainerName, "hot", correlationId, result, ex, cancellationToken);

            offsetTracker.Complete(result.TopicPartitionOffset);
            return;
        }

        var (sessionId, messageId, body) = MapToServiceBusMessage(value);

        var message = new ServiceBusMessage(body)
        {
            // Deterministic id from the event payload - this is what makes the Service Bus
            // consumer's dedupe check on redelivery actually work.
            MessageId = messageId,
            SessionId = sessionId,
        };
        message.ApplicationProperties["CorrelationId"] = correlationId;

        var pipeline = pipelineProvider.GetPipeline(ResiliencePipelines.ServiceBusPublish);

        // Deliberately not caught here - an unrecoverable Service Bus publish failure propagates to
        // RunWorkerAsync and stops this consumer (see the class-level remarks for why this is handled
        // differently from a deserialization failure above).
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

    /// <summary>
    /// Writes one message's raw Kafka headers and body to the given blob storage container, at
    /// <c>{correlationId}/{ConsumerName}/{SchemaName}/{timestamp}_{guid}.log</c>. Best-effort - a
    /// Blob Storage outage (after the upload pipeline's own retries are exhausted) is logged and
    /// swallowed rather than blocking the dedup check or the relay itself; the audit trail is a
    /// diagnostic aid, not the durability boundary (Service Bus is, per integration-resiliency.instructions.md §1).
    /// </summary>
    private async Task WriteAuditLogAsync(
        string containerName,
        string tier,
        string correlationId,
        ConsumeResult<string, byte[]> result,
        Exception? exception,
        CancellationToken cancellationToken)
    {
        var blobName = $"{correlationId}/{ConsumerName}/{SchemaName}/{DateTime.UtcNow:yyyyMMddHHmmssfff}_{Guid.NewGuid():N}.log";

        var record = new KafkaAuditRecord(
            Topic: result.Topic,
            Partition: result.Partition.Value,
            Offset: result.Offset.Value,
            CorrelationId: correlationId,
            Headers: result.Message.Headers?
                .Select(header => new KafkaAuditHeader(header.Key, Encoding.UTF8.GetString(header.GetValueBytes())))
                .ToArray() ?? [],
            BodyBase64: result.Message.Value is { } bytes ? Convert.ToBase64String(bytes) : null,
            Exception: exception?.ToString());

        try
        {
            using var stream = new MemoryStream(JsonSerializer.SerializeToUtf8Bytes(record));
            await fileStore.UploadAsync(containerName, blobName, stream, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "{ConsumerName}: failed to write {Tier}-tier audit log {ContainerName}/{BlobName} - continuing without it. CorrelationId: {CorrelationId}",
                ConsumerName, tier, containerName, blobName, correlationId);
        }
    }

    private sealed record KafkaAuditHeader(string Key, string Value);

    private sealed record KafkaAuditRecord(
        string Topic, int Partition, long Offset, string CorrelationId,
        KafkaAuditHeader[] Headers, string? BodyBase64, string? Exception);

    /// <summary>Reads a Kafka header's value as a UTF-8 string, if present.</summary>
    /// <param name="headers">Headers on the consumed Kafka message, or <see langword="null"/> if none were sent.</param>
    /// <param name="key">Header name - see <see cref="KafkaHeaderNames"/>.</param>
    /// <returns>The header value, or <see langword="null"/> if absent.</returns>
    private static string? TryGetHeader(Headers? headers, string key)
    {
        return headers is not null && headers.TryGetLastBytes(key, out var bytes)
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
