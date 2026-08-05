using IIS.WMS.Common.Logging;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped.Mappers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped;

/// <summary>
/// Relays the single Avro <c>net.pandora.nexus.event.b2b.sales.ConsolidatedOrderShipped</c> event from
/// Kafka onto the durable Azure Service Bus queue (docs/events/b2b.sales.ConsolidatedOrderShipped.md),
/// built on the shared <see cref="KafkaConsumerHostedServiceBase"/> - handles exactly one schema
/// regardless of the Kafka <c>Type</c> header's value (registered under
/// <see cref="KafkaConsumerHostedServiceBase.DefaultEventType"/>), same as
/// <see cref="OrderStatusChanged.OrderStatusChangedConsumerHostedService"/>. Overrides
/// <c>getServiceBusRouting</c> with a computed SessionId (see <see cref="BuildSessionId"/>'s remarks for
/// the doc deviation) - <c>MessageId</c> is the Kafka record key itself, never a freshly generated Guid,
/// so redelivery dedupe works. Overrides <c>validateAsync</c> to reject (throw) a null/empty
/// <c>WarehouseCode</c> or an empty <c>ShipmentLines</c> collection (doc §7 validation table - both are
/// hard validation failures that must DeadLetter).
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class ConsolidatedOrderShippedConsumerHostedService : KafkaConsumerHostedServiceBase
{
    /// <param name="options">Topic, consumer group, Schema Registry URL, and Service Bus queue settings for this consumer.</param>
    /// <param name="specificRecordDeserializerFactory">Builds the Avro deserializer and its backing Schema Registry client.</param>
    /// <param name="infrastructure">The Service Bus client, Polly pipeline provider, hot/cold file stores, Blob Storage options, and dedup service every consumer shares.</param>
    /// <param name="logger">Logger for consume/relay/poison-message/validation-rejection events.</param>
    public ConsolidatedOrderShippedConsumerHostedService(
        IOptions<ConsolidatedOrderShippedConsumerOptions> options,
        ISpecificRecordDeserializerFactory specificRecordDeserializerFactory,
        ConsumerRelayInfrastructure infrastructure,
        ILogger<ConsolidatedOrderShippedConsumerHostedService> logger)
        : base(options.Value, infrastructure, logger, specificRecordDeserializerFactory)
    {
        RegisterSchemaHandlers(new Dictionary<string, ISchemaHandler>
        {
            [DefaultEventType] = CreateSchemaHandler<
                net.pandora.nexus.@event.b2b.sales.ConsolidatedOrderShipped,
                ConsolidatedOrderShippedEvent>(
                ConsolidatedOrderShippedEventMapper.ToConsolidatedOrderShippedEvent,
                getServiceBusRouting: (value, key) => (
                    BuildSessionId(value),
                    key ?? throw new InvalidOperationException("Missing Kafka record key for this ConsolidatedOrderShipped event - required to route onto Service Bus.")),
                validateAsync: ValidateAsync),
        });
    }

    /// <summary>
    /// Deterministic per-shipment SessionId so every Service Bus message for the same shipment lands in
    /// the same session.
    /// </summary>
    /// <remarks>
    /// TODO(ai): unresolved precedence conflict - doc §2/§7 specifies <c>SessionId = {FulfilmentId}:{ItemCode}</c>,
    /// but the Kafka payload is one message per shipment carrying an array of ShipmentLines (each with its
    /// own ItemCode/ProductId) - there is no single ItemCode to key on. Relaying 1:1 per Kafka message with
    /// <c>SessionId = {WarehouseCode}:{Shipment.Id ?? ParentOrderId}</c> per explicit user direction; the
    /// per-line B2B confirmation fan-out happens in-process inside <see cref="Handlers.ConsolidatedOrderShippedHandler"/>
    /// instead of at the relay layer. Review before shipping.
    /// </remarks>
    private static string BuildSessionId(ConsolidatedOrderShippedEvent value) =>
        $"{value.Shipment.WarehouseCode}:{(string.IsNullOrEmpty(value.Shipment.Id) ? value.ParentOrderId : value.Shipment.Id)}";

    /// <summary>
    /// Rejects (throws) a null/empty <see cref="ConsolidatedOrderShipment.WarehouseCode"/> or an empty
    /// <see cref="ConsolidatedOrderShipment.ShipmentLines"/> collection (doc §7 validation table) - both
    /// are malformed data that must DeadLetter, never a silent skip.
    /// </summary>
    private static Task<bool> ValidateAsync(ConsolidatedOrderShippedEvent value, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(value.Shipment.WarehouseCode))
        {
            throw new InvalidOperationException(
                $"ConsolidatedOrderShipped event for ParentOrderId '{value.ParentOrderId}' has a null/empty WarehouseCode - cannot classify or route.");
        }

        if (value.Shipment.ShipmentLines.Count == 0)
        {
            throw new InvalidOperationException(
                $"ConsolidatedOrderShipped event for ParentOrderId '{value.ParentOrderId}' has no ShipmentLines - nothing to confirm.");
        }

        return Task.FromResult(true);
    }
}
