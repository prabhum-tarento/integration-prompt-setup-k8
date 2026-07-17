using System.Runtime.CompilerServices;
using Azure.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure.TestDoubles;

/// <summary>
/// In-process <see cref="ServiceBusSender"/> - built via the SDK's own parameterless constructor
/// (provided specifically for mocking/subclassing, per Azure SDK guidelines), the same pattern this
/// repo's integration tests already use for <c>VirtualServiceBusSender</c>
/// (integration-resiliency.instructions.md §9), mirrored here for a unit test that has no
/// <c>IIS.WMS.Consumer.IntegrationTests</c> project reference to reuse that type directly. Records
/// every message/batch <see cref="ServiceBusRelayPublisher"/> sends instead of making a real network
/// call, and can be configured (<see cref="OnSend"/>) to throw so a test can exercise the caller's
/// Polly <c>service-bus-publish</c> retry/exhaustion behavior (integration-resiliency.instructions.md §3).
/// </summary>
public sealed class FakeServiceBusSender(string queueName) : ServiceBusSender
{
    private readonly ConditionalWeakTable<ServiceBusMessageBatch, List<ServiceBusMessage>> batchBackingStores = new();

    /// <summary>The queue name this sender was created for - lets a test assert which queue a multi-queue batch publish actually used.</summary>
    public string QueueName { get; } = queueName;

    /// <summary>Every message sent via <see cref="SendMessageAsync"/> or as part of a batch, in send order.</summary>
    public List<ServiceBusMessage> SentMessages { get; } = [];

    /// <summary>The message count of every batch actually sent via <see cref="SendMessagesAsync(ServiceBusMessageBatch, CancellationToken)"/>, in send order.</summary>
    public List<int> SentBatchSizes { get; } = [];

    /// <summary>How many times <see cref="DisposeAsync"/> has been called - lets a test assert <see cref="ServiceBusRelayPublisher.ClearServiceBusSendersAsync"/> disposed a cached sender exactly once, not once per caller.</summary>
    public int DisposeCount { get; private set; }

    /// <summary>Whether <see cref="DisposeAsync"/> has been called at least once.</summary>
    public bool Disposed => DisposeCount > 0;

    /// <summary>
    /// Messages a batch this sender creates accepts before <c>TryAddMessage</c> starts returning
    /// <see langword="false"/> - lower this to force <see cref="ServiceBusRelayPublisher"/>'s
    /// batch-chunking path, or set to 0 to force its "doesn't fit even alone" failure path.
    /// </summary>
    public int MaxMessagesPerBatch { get; set; } = 1_000;

    /// <summary>
    /// Invoked for every message immediately before it's recorded as sent - throw from this (e.g. a
    /// transient <see cref="ServiceBusException"/>) to simulate a publish failure a caller's Polly
    /// pipeline should retry/exhaust against. Defaults to a no-op (every send succeeds).
    /// </summary>
    public Action<ServiceBusMessage> OnSend { get; set; } = static _ => { };

    /// <inheritdoc />
    public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
    {
        OnSend(message);
        SentMessages.Add(message);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override Task SendMessagesAsync(IEnumerable<ServiceBusMessage> messages, CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
        {
            OnSend(message);
            SentMessages.Add(message);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
    public override Task SendMessagesAsync(ServiceBusMessageBatch messageBatch, CancellationToken cancellationToken = default)
    {
        if (!batchBackingStores.TryGetValue(messageBatch, out var messages))
        {
            throw new InvalidOperationException("This ServiceBusMessageBatch wasn't created by this FakeServiceBusSender.");
        }

        SentBatchSizes.Add(messages.Count);
        return SendMessagesAsync(messages, cancellationToken);
    }

    /// <inheritdoc />
    public override ValueTask DisposeAsync()
    {
        DisposeCount++;
        return ValueTask.CompletedTask;
    }
}
