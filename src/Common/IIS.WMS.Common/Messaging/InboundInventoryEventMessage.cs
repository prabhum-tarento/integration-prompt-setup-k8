namespace IIS.WMS.Common.Messaging;

/// <summary>
/// Wire contract carried end to end from the Kafka topic through the Service Bus relay
/// (integration-resiliency.instructions.md §1) - shared between the Kafka-side relay (the planned
/// Producer project) and the Service Bus-side consumer. <see cref="EventId"/> must be deterministic
/// (derived from the source payload or Kafka partition/offset, never a freshly generated GUID) -
/// it becomes both the outbound Service Bus message id and, for a Reserve event, the aggregate's
/// reservation id, which is what makes redelivery a no-op instead of a double-decrement. Carries
/// no correlation id of its own - per integration-resiliency.instructions.md §4, that travels via
/// the Kafka message header, then onto the Service Bus message as both
/// <c>ApplicationProperties["CorrelationId"]</c> (the transport-hop property) and the
/// <see cref="ServiceBusRelayEnvelope"/> that wraps this type's own JSON as its
/// <see cref="ServiceBusRelayEnvelope.ReflexSchema"/>, not a field on this record itself.
/// </summary>
/// <param name="EventId">Deterministic id for this event - see the type-level remarks.</param>
/// <param name="WarehouseId">Warehouse the event applies to.</param>
/// <param name="Sku">SKU the event applies to.</param>
/// <param name="Quantity">Quantity carried by the event - meaning depends on <paramref name="EventType"/>.</param>
/// <param name="EventType">Discriminator for how the event should be applied (e.g. <c>"Create"</c>, <c>"Reserve"</c>).</param>
public sealed record InboundInventoryEventMessage(
    string EventId, string WarehouseId, string Sku, int Quantity, string EventType);
