using Azure.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure.TestDoubles;

/// <summary>
/// In-process <see cref="ServiceBusClient"/> - built via the SDK's own parameterless constructor
/// (provided specifically for mocking/subclassing, per Azure SDK guidelines), mirroring this repo's
/// integration-test <c>VirtualServiceBusClient</c> (integration-resiliency.instructions.md §9) for a
/// unit test that has no <c>IIS.WMS.Consumer.IntegrationTests</c> project reference to reuse that type
/// directly. Hands back a <see cref="FakeServiceBusSender"/> per queue instead of connecting to a real
/// namespace - used both to satisfy the <see cref="ServiceBusClient"/> constructor parameter every
/// hosted service under test takes (never exercised, since these tests call <c>HandleMessageAsync</c>
/// directly rather than <c>ExecuteAsync</c>) and, for <see cref="ServiceBusRelayPublisher"/>, as the
/// sender factory its tests assert against.
/// </summary>
public sealed class FakeServiceBusClient : ServiceBusClient
{
    private readonly Dictionary<string, FakeServiceBusSender> createdSenders = [];

    /// <summary>Every <see cref="FakeServiceBusSender"/> this client has created, keyed by queue name.</summary>
    public IReadOnlyDictionary<string, FakeServiceBusSender> CreatedSenders => createdSenders;

    /// <inheritdoc />
    public override ServiceBusSender CreateSender(string queueOrTopicName) => CreateAndTrackSender(queueOrTopicName);

    /// <inheritdoc />
    public override ServiceBusSender CreateSender(string queueOrTopicName, ServiceBusSenderOptions options) => CreateAndTrackSender(queueOrTopicName);

    private FakeServiceBusSender CreateAndTrackSender(string queueOrTopicName)
    {
        var sender = new FakeServiceBusSender(queueOrTopicName);
        createdSenders[queueOrTopicName] = sender;
        return sender;
    }

    /// <inheritdoc />
    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
