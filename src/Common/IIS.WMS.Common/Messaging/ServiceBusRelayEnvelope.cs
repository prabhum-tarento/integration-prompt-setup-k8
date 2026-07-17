using System.Text.Json;
using IIS.WMS.Common.Logging;

namespace IIS.WMS.Common.Messaging;

/// <summary>
/// Body-level envelope the Kafka-side relay (the planned Producer project) wraps every schema's
/// payload inside (integration-resiliency.instructions.md §4) - carries the full correlation context
/// the Kafka consumer built (<see cref="CorrelationId"/>, <see cref="AppId"/>, <see cref="Type"/>)
/// alongside the schema's own JSON as <see cref="ReflexSchema"/>, so the Service Bus consumer can
/// rebuild that same context without a second lookup. <see cref="CorrelationId"/> is also set as the
/// message's <c>ApplicationProperties["CorrelationId"]</c> - that transport-hop property is what a
/// broker-level filter/log line reads without deserializing the body; this envelope is what the
/// downstream consumer actually unwraps.
/// </summary>
public sealed class ServiceBusRelayEnvelope
{
    /// <summary>The relayed message's correlation id - see <c>ICorrelationContext.CorrelationId</c>.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>The producing application's id - see <c>ICorrelationContext.AppId</c>.</summary>
    public string? AppId { get; init; }
    public LogCriteria? LogCriteria { get; init; }
    public string? EntityType { get; init; }

    /// <summary>The Kafka event type and relaying consumer's name - see <c>ICorrelationContext.Types</c>.</summary>
    public string? Type { get; init; }

    /// <summary>The schema handler's own JSON for this event - deserialize into that schema's wire contract (e.g. <see cref="InboundInventoryEventMessage"/>).</summary>
    public JsonElement ReflexSchema { get; init; }
    public string BlobPath { get; set; } = string.Empty;
}
