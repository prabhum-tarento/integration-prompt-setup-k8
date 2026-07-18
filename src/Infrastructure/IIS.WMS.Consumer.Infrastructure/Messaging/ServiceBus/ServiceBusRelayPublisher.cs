using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Resilience;
using IIS.WMS.Consumer.Application.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly.Registry;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;

/// <summary>
/// Shared <see cref="IServiceBusRelayPublisher"/> implementation - the one place a
/// <see cref="ServiceBusRelayEnvelope"/> is built, an oversized payload is claim-check offloaded to
/// blob storage, and a <see cref="ServiceBusMessage"/> is sent through
/// <see cref="ResiliencePipelines.ServiceBusPublish"/>. Registered as a singleton and shared by every
/// caller in this process (Kafka relay consumers, Service Bus-side handlers, application services)
/// rather than one instance per caller, so <see cref="ServiceBusSender"/>s are cached and reused per
/// Microsoft's Service Bus client-lifetime guidance - one sender per distinct queue actually used
/// across the whole process, not one per caller that happens to publish to the same queue.
/// Implements <see cref="IServiceBusSenderCacheSource"/> itself for the same reason: the admin
/// endpoint over cached senders (<c>ServiceBusSendersController</c>) now reports this one shared cache
/// instead of one entry per Kafka consumer, since sender caching itself is no longer split per caller.
/// </summary>
public sealed class ServiceBusRelayPublisher(
    ServiceBusClient serviceBusClient,
    ResiliencePipelineProvider<string> pipelineProvider,
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.HotTierKey)] IFileStore hotFileStore,
    IOptions<BlobStorageOptions> blobStorageOptions,
    ILogger<ServiceBusRelayPublisher> logger)
    : IServiceBusRelayPublisher, IServiceBusSenderCacheSource, IAsyncDisposable
{
    /// <summary>
    /// Hardcoded default claim-check threshold, in bytes, for a <see cref="ServiceBusRelayMessage"/>
    /// that doesn't supply its own <see cref="ServiceBusRelayMessage.MaxMessageSizeBytesOverride"/> -
    /// 200 KiB, a conservative margin under Service Bus Standard tier's 256 KB per-message limit that
    /// leaves headroom for <see cref="ServiceBusRelayEnvelope"/>'s own fields wrapped around the
    /// payload. A Kafka consumer's own <c>ConsumerOptions.MaxServiceBusMessageSizeBytes</c> is a
    /// separate, independently configurable setting that resolves to the same value by default and is
    /// passed through as the per-message override - see <c>ConsumerHostedService</c>.
    /// </summary>
    public const int DefaultMaxMessageSizeBytes = 200 * 1024;

    private static readonly JsonElement EmptyReflexSchema = CreateEmptyReflexSchema();

    private readonly BlobStorageOptions blobStorageOptions = blobStorageOptions.Value;
    private readonly ConcurrentDictionary<string, ServiceBusSender> senders = new();
    private bool disposed;

    /// <inheritdoc />
    public string ConsumerName => nameof(ServiceBusRelayPublisher);

    /// <inheritdoc />
    public IReadOnlyCollection<string> CachedServiceBusSenderQueueNames => [.. senders.Keys];

    /// <summary>Builds <see cref="EmptyReflexSchema"/> once - the backing <see cref="JsonDocument"/> only needs to live long enough for <see cref="JsonElement.Clone"/> to copy its data out.</summary>
    private static JsonElement CreateEmptyReflexSchema()
    {
        using var document = JsonDocument.Parse("{}");
        return document.RootElement.Clone();
    }

    /// <inheritdoc />
    public async Task<ServiceBusRelayPublishResult> PublishAsync(ServiceBusRelayMessage message, CancellationToken cancellationToken = default)
    {
        var (serviceBusMessage, result) = await BuildMessageAsync(message, cancellationToken);
        var sender = GetSender(message.QueueName);
        var pipeline = pipelineProvider.GetPipeline(ResiliencePipelines.ServiceBusPublish);

        var publishStopwatch = Stopwatch.StartNew();
        await pipeline.ExecuteAsync(async ct => await sender.SendMessageAsync(serviceBusMessage, ct), cancellationToken);
        var publishDuration = publishStopwatch.Elapsed;

        logger.LogInformation(
            "Relayed {MessageId} for session {SessionId} to Service Bus queue {QueueName} in {PublishDurationMs}ms. CorrelationId: {CorrelationId}",
            message.MessageId, message.SessionId, message.QueueName, publishDuration.TotalMilliseconds, message.CorrelationId);

        return result with { PublishDuration = publishDuration };
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ServiceBusRelayPublishResult>> PublishBatchAsync(
        IReadOnlyCollection<ServiceBusRelayMessage> messages, CancellationToken cancellationToken = default)
    {
        if (messages.Count == 0)
        {
            return [];
        }

        var results = new ServiceBusRelayPublishResult[messages.Count];
        var indexed = messages.Select((message, index) => (Message: message, Index: index));

        foreach (var group in indexed.GroupBy(item => item.Message.QueueName))
        {
            await PublishBatchToQueueAsync(group.Key, [.. group], results, cancellationToken);
        }

        return results;
    }

    /// <summary>
    /// Relays every message bound for one queue, packed into as few <see cref="ServiceBusMessageBatch"/>es
    /// as fit within the SDK's own per-batch size limit - builds every message up front (running the
    /// claim-check for each), then repeatedly fills a batch via <see cref="ServiceBusMessageBatch.TryAddMessage"/>
    /// until it's full or every message for this queue has been added, sending and disposing each batch
    /// before starting the next (Microsoft's documented "send batches of messages" pattern).
    /// </summary>
    private async Task PublishBatchToQueueAsync(
        string queueName,
        IReadOnlyList<(ServiceBusRelayMessage Message, int Index)> items,
        ServiceBusRelayPublishResult[] results,
        CancellationToken cancellationToken)
    {
        var sender = GetSender(queueName);
        var pipeline = pipelineProvider.GetPipeline(ResiliencePipelines.ServiceBusPublish);

        var built = await BuildMessagesAsync(items, cancellationToken);

        var position = 0;

        while (position < built.Length)
        {
            using var batch = await sender.CreateMessageBatchAsync(cancellationToken);
            var batchStart = position;

            while (position < built.Length && batch.TryAddMessage(built[position].ServiceBusMessage))
            {
                position++;
            }

            if (position == batchStart)
            {
                throw new InvalidOperationException(
                    $"Message '{built[position].ServiceBusMessage.MessageId}' for queue '{queueName}' does not fit in a Service Bus batch on its own, even after claim-check offload.");
            }

            var publishStopwatch = Stopwatch.StartNew();
            await pipeline.ExecuteAsync(async ct => await sender.SendMessagesAsync(batch, ct), cancellationToken);
            var publishDuration = publishStopwatch.Elapsed;

            logger.LogInformation(
                "Relayed a batch of {BatchCount} message(s) to Service Bus queue {QueueName} in {PublishDurationMs}ms.",
                batch.Count, queueName, publishDuration.TotalMilliseconds);

            for (var i = batchStart; i < position; i++)
            {
                results[built[i].Index] = built[i].Result with { PublishDuration = publishDuration };
            }
        }
    }

    /// <summary>Builds every message for one queue's batch up front (running the claim-check for each), before any batch is packed.</summary>
    private async Task<(ServiceBusMessage ServiceBusMessage, ServiceBusRelayPublishResult Result, int Index)[]> BuildMessagesAsync(
        IReadOnlyList<(ServiceBusRelayMessage Message, int Index)> items, CancellationToken cancellationToken)
    {
        var built = new (ServiceBusMessage ServiceBusMessage, ServiceBusRelayPublishResult Result, int Index)[items.Count];

        for (var i = 0; i < items.Count; i++)
        {
            var (serviceBusMessage, result) = await BuildMessageAsync(items[i].Message, cancellationToken);
            built[i] = (serviceBusMessage, result, items[i].Index);
        }

        return built;
    }

    /// <summary>
    /// Builds the <see cref="ServiceBusMessage"/> for one <see cref="ServiceBusRelayMessage"/>: claim-check
    /// (inline vs. blob-offloaded payload), <see cref="ServiceBusRelayEnvelope"/> construction, and the
    /// SDK message's <c>MessageId</c>/<c>SessionId</c>/<c>ApplicationProperties["CorrelationId"]</c>. A
    /// blob upload failure here propagates as an exception - see this type's own remarks.
    /// </summary>
    private async Task<(ServiceBusMessage ServiceBusMessage, ServiceBusRelayPublishResult Result)> BuildMessageAsync(
        ServiceBusRelayMessage message, CancellationToken cancellationToken)
    {
        var (reflexSchema, blobPath, blobOffloadDuration, wasOffloaded) = await ResolveReflexSchemaAsync(message, cancellationToken);

        var envelope = new ServiceBusRelayEnvelope
        {
            CorrelationId = message.CorrelationId,
            AppId = message.AppId,
            LogCriteria = message.LogCriteria,
            EntityType = message.EntityType,
            Type = JsonSerializer.Serialize(message.Types?.Distinct().ToList()),
            ReflexSchema = reflexSchema,
            BlobPath = blobPath,
        };

        var serviceBusMessage = new ServiceBusMessage(JsonSerializer.Serialize(envelope))
        {
            // Deterministic id from the event payload - this is what makes the downstream consumer's
            // own dedupe check on redelivery actually work.
            MessageId = message.MessageId,
            SessionId = message.SessionId,
        };
        serviceBusMessage.ApplicationProperties["CorrelationId"] = message.CorrelationId;

        return (serviceBusMessage, new ServiceBusRelayPublishResult(wasOffloaded, blobPath, blobOffloadDuration, TimeSpan.Zero));
    }

    /// <summary>
    /// The claim-check decision for one message's payload: inline as a parsed <see cref="JsonElement"/>
    /// when it fits under the size limit, or uploaded to blob storage (returning <see cref="EmptyReflexSchema"/>
    /// plus the blob path) when it doesn't.
    /// </summary>
    private async Task<(JsonElement ReflexSchema, string BlobPath, TimeSpan BlobOffloadDuration, bool WasOffloaded)> ResolveReflexSchemaAsync(
        ServiceBusRelayMessage message, CancellationToken cancellationToken)
    {
        var payloadSizeBytes = Encoding.UTF8.GetByteCount(message.Json);
        var maxMessageSizeBytes = message.MaxMessageSizeBytesOverride ?? DefaultMaxMessageSizeBytes;

        if (payloadSizeBytes <= maxMessageSizeBytes)
        {
            using var payloadDocument = JsonDocument.Parse(message.Json);
            return (payloadDocument.RootElement.Clone(), string.Empty, TimeSpan.Zero, false);
        }

        var blobOffloadStopwatch = Stopwatch.StartNew();

        using var payloadStream = new MemoryStream(Encoding.UTF8.GetBytes(message.Json));
        var blobPath = await hotFileStore.UploadAsync(
            blobStorageOptions.LargePayloadContainerName,
            $"{message.CorrelationId}/{message.PayloadName}/{message.SourceName}/{DateTime.UtcNow:yyyyMMddHHmmssfff}_{Guid.NewGuid():N}.json",
            payloadStream, cancellationToken);

        var blobOffloadDuration = blobOffloadStopwatch.Elapsed;

        logger.LogInformation(
            "Payload for {MessageId} was {PayloadSizeBytes} bytes (limit {MaxMessageSizeBytes}) - offloaded to blob {BlobPath} in {BlobOffloadDurationMs}ms. CorrelationId: {CorrelationId}",
            message.MessageId, payloadSizeBytes, maxMessageSizeBytes, blobPath, blobOffloadDuration.TotalMilliseconds, message.CorrelationId);

        return (EmptyReflexSchema, blobPath, blobOffloadDuration, true);
    }

    /// <summary>Resolves (creating and caching on first use) the <see cref="ServiceBusSender"/> for <paramref name="queueName"/> - one sender per distinct queue name actually used across every caller, not one per caller.</summary>
    private ServiceBusSender GetSender(string queueName) => senders.GetOrAdd(queueName, serviceBusClient.CreateSender);

    /// <inheritdoc />
    public async Task ClearServiceBusSendersAsync()
    {
        foreach (var queueName in senders.Keys.ToArray())
        {
            if (senders.TryRemove(queueName, out var sender))
            {
                await sender.DisposeAsync();
            }
        }
    }

    /// <summary>Disposes every cached <see cref="ServiceBusSender"/> - guarded so a caller that disposes this singleton more than once only closes each sender once.</summary>
    public async ValueTask DisposeAsync()
    {
        if (disposed)
        {
            return;
        }

        await ClearServiceBusSendersAsync();
        disposed = true;
        GC.SuppressFinalize(this);
    }
}
