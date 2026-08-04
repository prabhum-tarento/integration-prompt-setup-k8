using IIS.WMS.Common.Logging;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged.Mappers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged;

/// <summary>
/// Relays the single Avro <c>net.pandora.nexus.event.inventory.InternalHallmarkingStatusChanged</c>
/// event from Kafka onto the durable Azure Service Bus queue
/// (docs/events/inventory.InternalHallmarkingStatusChanged.md), built on the shared
/// <see cref="KafkaConsumerHostedServiceBase"/> - handles exactly one schema regardless of the
/// Kafka <c>Type</c> header's value (registered under <see cref="KafkaConsumerHostedServiceBase.DefaultEventType"/>),
/// same as <see cref="BulkInventoryImport.BulkInventoryImportConsumerHostedService"/>. Unlike
/// <see cref="InventoryEvents.InventoryEventConsumerHostedService"/>'s default Kafka-record-key
/// routing, this consumer overrides <c>getServiceBusRouting</c> with an explicit
/// <c>SessionId = {FulfilmentId}:{ItemCode}</c> (doc §5.6) so that every status transition for the
/// same item/fulfilment lands on the same Service Bus session and is processed in order -
/// <c>MessageId</c> is this event's own deterministic <see cref="Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged.InternalHallmarkingStatusChangedEvent.Id"/>,
/// never a freshly generated Guid, so that redelivery dedupe works (same rationale as
/// <see cref="BulkInventoryImport.BulkInventoryImportConsumerHostedService"/>'s own remarks).
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class InternalHallmarkingStatusChangedConsumerHostedService : KafkaConsumerHostedServiceBase
{
    /// <summary>Builds the schema-registry-backed Avro consumer and the Service Bus sender it relays onto.</summary>
    /// <param name="options">Topic, consumer group, Schema Registry URL, and Service Bus queue settings for this consumer.</param>
    /// <param name="specificRecordDeserializerFactory">Builds the Avro deserializer and its backing Schema Registry client.</param>
    /// <param name="infrastructure">The Service Bus client, Polly pipeline provider, hot/cold file stores, Blob Storage options, and dedup service every consumer shares - see <see cref="ConsumerRelayInfrastructure"/>.</param>
    /// <param name="logger">Logger for consume/relay/poison-message events.</param>
    public InternalHallmarkingStatusChangedConsumerHostedService(
        IOptions<InternalHallmarkingStatusChangedConsumerOptions> options,
        ISpecificRecordDeserializerFactory specificRecordDeserializerFactory,
        ConsumerRelayInfrastructure infrastructure,
        ILogger<InternalHallmarkingStatusChangedConsumerHostedService> logger)
        : base(options.Value, infrastructure, logger, specificRecordDeserializerFactory)
    {
        RegisterSchemaHandlers(new Dictionary<string, ISchemaHandler>
        {
            [DefaultEventType] = CreateSchemaHandler<
                net.pandora.nexus.@event.inventory.InternalHallmarkingStatusChanged,
                InternalHallmarkingStatusChangedEvent>(
                InternalHallmarkingStatusChangedEventMapper.ToInternalHallmarkingStatusChangedEvent,
                // SessionId = {FulfilmentId}:{ItemCode} (doc §5.6) so every status transition for the
                // same item/fulfilment is processed in order on the Service Bus consumer side.
                // MessageId is the event's own deterministic Id, not a fresh Guid, for redelivery dedupe.
                (value, _) => ($"{value.Location.Id}:{value.ItemLine.ProductId}", value.Id)),
        });
    }
}
