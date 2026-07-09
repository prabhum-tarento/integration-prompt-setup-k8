namespace IIS.WMS.Consumer.Infrastructure.Messaging;

/// <summary>
/// Wire contract carried end to end from the Kafka topic through the Service Bus relay
/// (integration-resiliency.instructions.md §1). <see cref="EventId"/> must be deterministic
/// (derived from the source payload or Kafka partition/offset, never a freshly generated GUID) -
/// it becomes both the outbound Service Bus message id and, for a Reserve event, the aggregate's
/// reservation id, which is what makes redelivery a no-op instead of a double-decrement. Carries
/// no correlation id of its own - per integration-resiliency.instructions.md §4, that travels via
/// the Kafka message header (read by <see cref="Kafka.ConsumerHostedService{TValue}"/>) and the
/// Service Bus <c>ApplicationProperties["CorrelationId"]</c> it's relayed onto, not the body.
/// </summary>
/// <param name="EventId">Deterministic id for this event - see the type-level remarks.</param>
/// <param name="WarehouseId">Warehouse the event applies to.</param>
/// <param name="Sku">SKU the event applies to.</param>
/// <param name="Quantity">Quantity carried by the event - meaning depends on <paramref name="EventType"/>.</param>
/// <param name="EventType">Discriminator for how the event should be applied (e.g. <c>"Create"</c>, <c>"Reserve"</c>).</param>
public sealed record InboundInventoryEventMessage(
    string EventId, string WarehouseId, string Sku, int Quantity, string EventType);
