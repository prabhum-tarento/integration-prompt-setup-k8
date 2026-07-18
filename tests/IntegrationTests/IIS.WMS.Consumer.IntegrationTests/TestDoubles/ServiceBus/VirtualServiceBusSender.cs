using System.Runtime.CompilerServices;
using Azure.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles.ServiceBus;

/// <summary>
/// In-process <see cref="ServiceBusSender"/> - built via the SDK's own parameterless constructor
/// (provided specifically for mocking/subclassing, per Azure SDK guidelines), overriding
/// <see cref="SendMessageAsync"/>/<see cref="SendMessagesAsync(System.Collections.Generic.IEnumerable{ServiceBusMessage},System.Threading.CancellationToken)"/>
/// to forward into <see cref="VirtualServiceBusBroker.DispatchAsync"/> instead of a real network call
/// (integration-resiliency.instructions.md §9). <c>KafkaConsumerHostedServiceBase</c>'s (the Kafka relay)
/// existing sender-cache logic keeps working unmodified against whatever this returns - no production
/// code change needed for the publish side.
/// </summary>
/// <remarks>
/// Also overrides <see cref="CreateMessageBatchAsync(CancellationToken)"/>/
/// <see cref="SendMessagesAsync(ServiceBusMessageBatch,CancellationToken)"/> for
/// <c>ServiceBusRelayPublisher</c>'s bulk-send path - built via
/// <see cref="ServiceBusModelFactory.ServiceBusMessageBatch"/> (a real, public SDK factory method meant
/// exactly for this), capping each batch at <see cref="MaxMessagesPerBatch"/> messages via its
/// <c>tryAddCallback</c> rather than trying to estimate a real AMQP-encoded size with no live
/// connection - a test that needs to force multiple batches sets <see cref="MaxMessagesPerBatch"/>
/// lower. The <see cref="ConditionalWeakTable{TKey,TValue}"/> below correlates a batch back to the
/// message list backing it, since <see cref="ServiceBusMessageBatch"/> itself isn't enumerable.
/// </remarks>
public sealed class VirtualServiceBusSender(string queueName, VirtualServiceBusBroker broker) : ServiceBusSender
{
    private readonly ConditionalWeakTable<ServiceBusMessageBatch, List<ServiceBusMessage>> batchBackingStores = new();
    private readonly List<int> sentBatchSizes = [];

    /// <summary>Messages a batch this sender creates accepts before <c>TryAddMessage</c> starts returning <see langword="false"/> - defaults high enough not to interfere with a test that isn't exercising batch-chunking itself.</summary>
    public int MaxMessagesPerBatch { get; set; } = 1_000;

    /// <summary>The message count of every batch this sender has actually sent via <see cref="SendMessagesAsync(ServiceBusMessageBatch,CancellationToken)"/>, in send order - lets a test assert on chunking (e.g. five messages capped at two per batch sent as <c>[2, 2, 1]</c>) without needing its own batch-level bookkeeping.</summary>
    public IReadOnlyList<int> SentBatchSizes => sentBatchSizes;

    public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default) =>
        broker.DispatchAsync(queueName, message, cancellationToken);

    public override async Task SendMessagesAsync(IEnumerable<ServiceBusMessage> messages, CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
        {
            await broker.DispatchAsync(queueName, message, cancellationToken);
        }
    }

    public override ValueTask<ServiceBusMessageBatch> CreateMessageBatchAsync(CancellationToken cancellationToken = default)
    {
        var backingStore = new List<ServiceBusMessage>();
        var batch = ServiceBusModelFactory.ServiceBusMessageBatch(
            batchSizeBytes: long.MaxValue,
            batchMessageStore: backingStore,
            tryAddCallback: _ => backingStore.Count < MaxMessagesPerBatch);

        batchBackingStores.Add(batch, backingStore);
        return ValueTask.FromResult(batch);
    }

    public override Task SendMessagesAsync(ServiceBusMessageBatch messageBatch, CancellationToken cancellationToken = default)
    {
        if (!batchBackingStores.TryGetValue(messageBatch, out var messages))
        {
            throw new InvalidOperationException("This ServiceBusMessageBatch wasn't created by this VirtualServiceBusSender.");
        }

        sentBatchSizes.Add(messages.Count);
        return SendMessagesAsync(messages, cancellationToken);
    }

    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
