using Azure.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles.ServiceBus;

/// <summary>
/// In-process <see cref="ServiceBusClient"/> - built via the SDK's own parameterless constructor
/// (provided specifically for mocking/subclassing, per Azure SDK guidelines), overriding
/// <see cref="CreateSender(string)"/>/<see cref="CreateSender(string,ServiceBusSenderOptions)"/> to hand
/// back a <see cref="VirtualServiceBusSender"/> bound to the shared <see cref="Broker"/>
/// (integration-resiliency.instructions.md §9). Register this in place of the real
/// <see cref="ServiceBusClient"/> singleton in an integration test's DI setup, the same way
/// <c>CustomWebApplicationFactory</c> already swaps out other Azure SDK clients.
/// </summary>
/// <remarks>
/// Deliberately does <b>not</b> override <c>CreateSessionProcessor</c>/<c>CreateProcessor</c>:
/// subscribing to either processor type's <c>ProcessMessageAsync</c> event throws on the subscription
/// itself when the processor wasn't built through a real connected client (a hard SDK limitation - see
/// <c>ServiceBusConsumerHostedService.ExecuteAsync</c>'s remarks), so no processor subclass could make
/// that safe anyway. Instead, <c>ServiceBusConsumerHostedService</c>/
/// <c>BulkImportServiceBusConsumerHostedService</c> build their processor lazily in <c>ExecuteAsync</c>,
/// which integration tests using this client never call - they wire <see cref="Broker"/> directly to
/// those classes' extracted <c>HandleMessageAsync</c> core method instead.
/// </remarks>
public sealed class VirtualServiceBusClient() : ServiceBusClient
{
    /// <summary>The shared in-process router every <see cref="VirtualServiceBusSender"/> this client creates publishes through - register consumer queues on this before sending.</summary>
    public VirtualServiceBusBroker Broker { get; } = new();

    /// <summary>
    /// Every <see cref="VirtualServiceBusSender"/> this client has created, keyed by queue name - lets a
    /// test reach back into the sender a cached-sender caller (e.g. <c>ServiceBusRelayPublisher</c>)
    /// already resolved, to tune <see cref="VirtualServiceBusSender.MaxMessagesPerBatch"/> for a
    /// bulk-batching scenario.
    /// </summary>
    public IReadOnlyDictionary<string, VirtualServiceBusSender> CreatedSenders => createdSenders;

    private readonly Dictionary<string, VirtualServiceBusSender> createdSenders = [];

    public override ServiceBusSender CreateSender(string queueOrTopicName) => CreateAndTrackSender(queueOrTopicName);

    public override ServiceBusSender CreateSender(string queueOrTopicName, ServiceBusSenderOptions options) => CreateAndTrackSender(queueOrTopicName);

    private VirtualServiceBusSender CreateAndTrackSender(string queueOrTopicName)
    {
        var sender = new VirtualServiceBusSender(queueOrTopicName, Broker);
        createdSenders[queueOrTopicName] = sender;
        return sender;
    }

    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
