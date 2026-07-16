using System.Text.Json;

namespace IIS.WMS.Consumer.Infrastructure.Messaging;

/// <summary>
/// Body-level envelope <see cref="Kafka.ConsumerHostedService"/> relays every schema's payload inside
/// (integration-resiliency.instructions.md §4) - carries the full <c>ICorrelationContext</c> the Kafka
/// consumer built (<see cref="CorrelationId"/>, <see cref="AppId"/>, <see cref="Types"/>) alongside the
/// schema's own JSON as <see cref="Payload"/>, so <see cref="ServiceBus.ServiceBusConsumerHostedService"/>
/// can rebuild that same context without a second lookup. <see cref="CorrelationId"/> is also set as the
/// message's <c>ApplicationProperties["CorrelationId"]</c> - that transport-hop property is what a
/// broker-level filter/log line reads without deserializing the body; this envelope is what the
/// downstream consumer actually unwraps.
/// </summary>
public sealed class ServiceBusRelayEnvelope
{
    /// <summary>The relayed message's correlation id - see <see cref="Application.Common.ICorrelationContext.CorrelationId"/>.</summary>
    public string CorrelationId { get; init; } = string.Empty;

    /// <summary>The producing application's id - see <see cref="Application.Common.ICorrelationContext.AppId"/>.</summary>
    public string AppId { get; init; } = string.Empty;

    /// <summary>The Kafka event type and relaying consumer's name - see <see cref="Application.Common.ICorrelationContext.Types"/>.</summary>
    public IReadOnlyList<string> Types { get; init; } = [];

    /// <summary>The schema handler's own JSON for this event - deserialize into that schema's wire contract (e.g. <see cref="InboundInventoryEventMessage"/>).</summary>
    public JsonElement Payload { get; init; }
}
