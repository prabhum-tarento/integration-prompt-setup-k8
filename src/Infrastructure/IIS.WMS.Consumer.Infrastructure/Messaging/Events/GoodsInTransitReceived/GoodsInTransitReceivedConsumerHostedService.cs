using IIS.WMS.Common.Logging;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived.Mappers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived;

/// <summary>
/// Relays the single Avro <c>net.pandora.nexus.event.b2b.purchase.GoodsInTransitReceived</c> event from
/// Kafka onto the durable Azure Service Bus queue (docs/events/b2b.purchase.GoodsInTransitReceived.md),
/// built on the shared <see cref="KafkaConsumerHostedServiceBase"/> - handles exactly one schema
/// regardless of the Kafka <c>Type</c> header's value (registered under
/// <see cref="KafkaConsumerHostedServiceBase.DefaultEventType"/>), same as
/// <see cref="OrderStatusChanged.OrderStatusChangedConsumerHostedService"/>. Overrides
/// <c>getServiceBusRouting</c> with the doc §2/§7.2-mandated <c>SessionId = {FulfilmentId}:{ItemCode}</c> -
/// <c>MessageId</c> is the Kafka record key itself, never a freshly generated Guid, so redelivery dedupe
/// works. Overrides <c>validateAsync</c> to reject (throw) a missing <c>PackingSlipId</c> or an empty
/// <c>ShipmentLines</c> collection (doc §7.4 - both are hard validation failures that must DeadLetter).
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class GoodsInTransitReceivedConsumerHostedService : KafkaConsumerHostedServiceBase
{
    /// <param name="options">Topic, consumer group, Schema Registry URL, and Service Bus queue settings for this consumer.</param>
    /// <param name="specificRecordDeserializerFactory">Builds the Avro deserializer and its backing Schema Registry client.</param>
    /// <param name="infrastructure">The Service Bus client, Polly pipeline provider, hot/cold file stores, Blob Storage options, and dedup service every consumer shares.</param>
    /// <param name="logger">Logger for consume/relay/poison-message/validation-rejection events.</param>
    public GoodsInTransitReceivedConsumerHostedService(
        IOptions<GoodsInTransitReceivedConsumerOptions> options,
        ISpecificRecordDeserializerFactory specificRecordDeserializerFactory,
        ConsumerRelayInfrastructure infrastructure,
        ILogger<GoodsInTransitReceivedConsumerHostedService> logger)
        : base(options.Value, infrastructure, logger, specificRecordDeserializerFactory)
    {
        RegisterSchemaHandlers(new Dictionary<string, ISchemaHandler>
        {
            [DefaultEventType] = CreateSchemaHandler<
                net.pandora.nexus.@event.b2b.purchase.GoodsInTransitReceived,
                GoodsInTransitReceivedEvent>(
                GoodsInTransitReceivedEventMapper.ToGoodsInTransitReceivedEvent,
                getServiceBusRouting: (value, key) => (
                    BuildSessionId(value),
                    key ?? throw new InvalidOperationException("Missing Kafka record key for this GoodsInTransitReceived event - required to route onto Service Bus.")),
                validateAsync: ValidateAsync),
        });
    }

    /// <summary>
    /// §2/§7.2 - <c>SessionId = {FulfilmentId}:{ItemCode}</c>. <c>FulfilmentId</c> is the §3.5-resolved
    /// destination node for the shipment as a whole; <c>ItemCode</c> comes from the first shipment line.
    /// </summary>
    /// <remarks>
    /// TODO(ai): SessionId derived from the first shipment line only - doc §7.2's sample message is
    /// single-line; multi-line-per-message SessionId semantics are undocumented.
    /// </remarks>
    private static string BuildSessionId(GoodsInTransitReceivedEvent value)
    {
        var fulfilmentId = GoodsInTransitReceivedRules.ResolveDestinationNode(value.Shipment.LocationTo, value.Shipment.WarehouseCode);
        var itemCode = value.Shipment.ShipmentLines.Count > 0 ? value.Shipment.ShipmentLines[0].ProductId : "UNKNOWN";

        return $"{fulfilmentId}:{itemCode}";
    }

    /// <summary>
    /// Rejects (throws) a null/empty <see cref="GoodsInTransitShipment.PackingSlipId"/> or an empty
    /// <see cref="GoodsInTransitShipment.ShipmentLines"/> collection (doc §7.4) - both are malformed data
    /// that must DeadLetter, never a silent skip.
    /// </summary>
    private static Task<bool> ValidateAsync(GoodsInTransitReceivedEvent value, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(value.Shipment.PackingSlipId))
        {
            throw new InvalidOperationException(
                "GoodsInTransitReceived event has a null/empty PackingSlipId - cannot classify or route.");
        }

        if (value.Shipment.ShipmentLines.Count == 0)
        {
            throw new InvalidOperationException(
                $"GoodsInTransitReceived event for PackingSlipId '{value.Shipment.PackingSlipId}' has no ShipmentLines - nothing to receive.");
        }

        return Task.FromResult(true);
    }
}
