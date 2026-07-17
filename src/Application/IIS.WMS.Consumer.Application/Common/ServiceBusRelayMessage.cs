using IIS.WMS.Common.Logging;

namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// One message to relay onto Service Bus via <see cref="IServiceBusRelayPublisher"/> - the raw JSON
/// payload plus the routing/correlation fields the publisher wraps into a
/// <c>ServiceBusRelayEnvelope</c> and a Service Bus SDK message. The caller never builds the envelope
/// or a Service Bus SDK message itself, and never decides whether the payload travels inline or is
/// claim-check offloaded to blob storage - the publisher decides that from <see cref="Json"/>'s size
/// against <see cref="MaxMessageSizeBytesOverride"/> (or its own default, if omitted).
/// </summary>
/// <param name="QueueName">Service Bus queue this message relays onto.</param>
/// <param name="SessionId">Session id Service Bus groups this message with related messages under (e.g. an aggregate key) - required if the target queue is session-enabled.</param>
/// <param name="MessageId">Deterministic message id, driving the downstream consumer's own idempotency check on redelivery - never a freshly generated id.</param>
/// <param name="CorrelationId">This message's correlation id - carried in the envelope body and, separately, as the transport-level <c>ApplicationProperties["CorrelationId"]</c>.</param>
/// <param name="AppId">The producing application's id, or <see langword="null"/> if not applicable.</param>
/// <param name="Types">Event/consumer type tags for the envelope's <c>Type</c> field - the publisher JSON-serializes this itself; pass the raw list.</param>
/// <param name="SourceName">The relaying consumer/service's own display name - one blob-path segment when this payload is claim-check offloaded.</param>
/// <param name="PayloadName">This payload's schema/kind name - the other blob-path segment when claim-check offloaded.</param>
/// <param name="Json">The payload to relay, already serialized to JSON.</param>
/// <param name="MaxMessageSizeBytesOverride">Overrides the publisher's own default claim-check threshold for this message, or <see langword="null"/> to use that default.</param>
/// <param name="LogCriteria">Optional log verbosity criteria to carry in the envelope, or <see langword="null"/> to omit.</param>
/// <param name="EntityType">Optional entity type to carry in the envelope, or <see langword="null"/> to omit.</param>
public sealed record ServiceBusRelayMessage(
    string QueueName,
    string SessionId,
    string MessageId,
    string CorrelationId,
    string? AppId,
    IReadOnlyList<string>? Types,
    string SourceName,
    string PayloadName,
    string Json,
    int? MaxMessageSizeBytesOverride = null,
    LogCriteria? LogCriteria = null,
    string? EntityType = null);
