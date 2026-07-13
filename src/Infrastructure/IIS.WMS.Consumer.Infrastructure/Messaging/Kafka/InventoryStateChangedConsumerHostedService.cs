using net.pandora.nexus.@event.inventory;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Relays the two Avro event types produced onto the shared <c>inventory-events</c> topic -
/// <c>net.pandora.nexus.event.inventory.InventoryStateChanged</c> and
/// <c>net.pandora.nexus.event.inventory.InventoryAdjusted</c> - from Kafka onto the durable Azure
/// Service Bus queue (integration-resiliency.instructions.md §1), built on the shared
/// <see cref="ConsumerHostedService"/> - the Avro counterpart to the JSON
/// <see cref="KafkaConsumerHostedService"/>. Registers one <see cref="ConsumerHostedService.ISchemaHandler"/>
/// per event type, keyed by its exact Kafka <c>Type</c> header value (<see cref="InventoryStateChangedEventType"/>/
/// <see cref="InventoryAdjustedEventType"/>) rather than <see cref="ConsumerHostedService.DefaultEventType"/> -
/// unlike every other consumer in this repo, this one cannot treat "any Type header value" as one schema, since the
/// two events it forwards are structurally unrelated Avro records. A message whose <c>Type</c> header
/// matches neither is dead-lettered as an unrecognized schema, same as any consumer (see the base
/// class's remarks). Both deserializers share one Schema Registry client - the base class's
/// <see cref="ConsumerHostedService.CreateSchemaHandler{TAvro,TValue}"/> builds it once, on the first
/// call, and reuses it for the second, rather than opening a second connection to the same registry.
/// </summary>
/// <remarks>
/// Both <c>InventoryStateChanged</c> and <c>InventoryAdjusted</c> are mapped into their own decoupled
/// wire contracts (<see cref="InventoryStateChangedEvent"/>/<see cref="InventoryAdjustedEvent"/>)
/// before anything downstream touches them - see <see cref="InventoryStateChangedEventMapper"/>/
/// <see cref="InventoryAdjustedEventMapper"/>. Both schemas route onto Service Bus using the Kafka
/// record key alone (the base class's default <c>RouteByEventKey</c>) - unlike a JSON contract keyed
/// on a compound <c>{WarehouseId}:{Sku}</c> SessionId derived from the payload, both these events carry
/// one <c>location</c> but an array of line items (<c>itemLines</c>/<c>adjustmentLines</c>, potentially
/// several products), so this relay forwards each event (including all its line items) as a single
/// Service Bus message keyed by the producer's own Kafka key, per team decision - a future per-line
/// fan-out would need a different SessionId derivation.
/// </remarks>
public sealed class InventoryStateChangedConsumerHostedService : ConsumerHostedService
{
    /// <summary>Builds the schema-registry-backed Avro consumer and the Service Bus sender it relays onto.</summary>
    /// <param name="options">
    /// Topic, consumer group, Schema Registry URL, and Service Bus queue settings for this consumer -
    /// including <see cref="InventoryStateChangedConsumerOptions.InventoryAdjustedServiceBusQueueName"/>,
    /// which lets <c>InventoryAdjusted</c> relay onto a different queue than
    /// <c>InventoryStateChanged</c>'s <see cref="ConsumerOptions.ServiceBusQueueName"/> if configured.
    /// </param>
    /// <param name="specificRecordDeserializerFactory">Builds the Avro deserializers and their shared backing Schema Registry client.</param>
    /// <param name="infrastructure">The Service Bus client, Polly pipeline provider, hot/cold file stores, Blob Storage options, and dedup service every consumer shares - see <see cref="ConsumerRelayInfrastructure"/>.</param>
    /// <param name="logger">Logger for consume/relay/poison-message events.</param>
    public InventoryStateChangedConsumerHostedService(
        IOptions<InventoryStateChangedConsumerOptions> options,
        ISpecificRecordDeserializerFactory specificRecordDeserializerFactory,
        ConsumerRelayInfrastructure infrastructure,
        ILogger<InventoryStateChangedConsumerHostedService> logger)
        : base(options.Value, infrastructure, logger, specificRecordDeserializerFactory)
    {
        RegisterSchemaHandlers(new Dictionary<string, ISchemaHandler>
        {
            [KafkaEvents.InventoryStateChangedEventType] = CreateSchemaHandler<InventoryStateChanged, InventoryStateChangedEvent>(
                InventoryStateChangedEventMapper.ToInventoryStateChangedEvent),
            [KafkaEvents.InventoryAdjustedEventType] = CreateSchemaHandler<InventoryAdjusted, InventoryAdjustedEvent>(
                InventoryAdjustedEventMapper.ToInventoryAdjustedEvent,
                // Null (the default) unless InventoryAdjustedServiceBusQueueName is configured -
                // falls back to the consumer-wide ServiceBusQueueName, so InventoryAdjusted lands
                // on the same queue as InventoryStateChanged until ops points it elsewhere.
                serviceBusQueueName: options.Value.InventoryAdjustedServiceBusQueueName),
        });
    }
}
