using IIS.WMS.Common.Logging;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged.Mappers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged;

/// <summary>
/// Relays the single Avro <c>net.pandora.nexus.event.b2b.sales.OrderStatusChanged</c> event from Kafka
/// onto the durable Azure Service Bus queue (docs/events/b2b.sales.OrderStatusChanged.md), built on the
/// shared <see cref="KafkaConsumerHostedServiceBase"/> - handles exactly one schema regardless of the
/// Kafka <c>Type</c> header's value (registered under <see cref="KafkaConsumerHostedServiceBase.DefaultEventType"/>),
/// same as <see cref="InternalHallmarkingStatusChanged.InternalHallmarkingStatusChangedConsumerHostedService"/>.
/// Overrides <c>getServiceBusRouting</c> with the doc §7-mandated <c>SessionId = {WarehouseCode}:{OrderId}</c> -
/// <c>MessageId</c> is the Kafka record key itself (the Avro schema's own <c>key:[orderId]</c> metadata),
/// never a freshly generated Guid, so redelivery dedupe works (doc §2/§7). Overrides <c>validateAsync</c>
/// to reject (throw, not return <see langword="false"/> - see
/// <see cref="KafkaConsumerHostedServiceBase.CreateSchemaHandler{TAvro,TValue}"/>'s remarks for the
/// throw-vs-return-false distinction) a null/empty <c>WarehouseCode</c> or an unresolved reference id
/// (doc §3.1/§3.2/§7/§8 - both are hard validation failures that must DeadLetter, not silently-skipped
/// valid data).
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class OrderStatusChangedConsumerHostedService : KafkaConsumerHostedServiceBase
{
    /// <summary>Builds the schema-registry-backed Avro consumer and the Service Bus sender it relays onto.</summary>
    /// <param name="options">Topic, consumer group, Schema Registry URL, and Service Bus queue settings for this consumer.</param>
    /// <param name="specificRecordDeserializerFactory">Builds the Avro deserializer and its backing Schema Registry client.</param>
    /// <param name="infrastructure">The Service Bus client, Polly pipeline provider, hot/cold file stores, Blob Storage options, and dedup service every consumer shares - see <see cref="ConsumerRelayInfrastructure"/>.</param>
    /// <param name="logger">Logger for consume/relay/poison-message/validation-rejection events.</param>
    public OrderStatusChangedConsumerHostedService(
        IOptions<OrderStatusChangedConsumerOptions> options,
        ISpecificRecordDeserializerFactory specificRecordDeserializerFactory,
        ConsumerRelayInfrastructure infrastructure,
        ILogger<OrderStatusChangedConsumerHostedService> logger)
        : base(options.Value, infrastructure, logger, specificRecordDeserializerFactory)
    {
        RegisterSchemaHandlers(new Dictionary<string, ISchemaHandler>
        {
            [DefaultEventType] = CreateSchemaHandler<
                net.pandora.nexus.@event.b2b.sales.OrderStatusChanged,
                OrderStatusChangedEvent>(
                OrderStatusChangedEventMapper.ToOrderStatusChangedEvent,
                getServiceBusRouting: (value, key) => (
                    $"{value.WarehouseCode}:{value.OrderId}",
                    key ?? throw new InvalidOperationException("Missing Kafka record key for this OrderStatusChanged event - required to route onto Service Bus.")),
                validateAsync: ValidateAsync),
        });
    }

    /// <summary>
    /// Rejects (throws) a null/empty <see cref="OrderStatusChangedEvent.WarehouseCode"/> or an
    /// unresolved reference id (doc §3.1/§3.2) - both are malformed data that must DeadLetter, never a
    /// silent skip. Valid data with an ordinary warehouse code and reference id always passes.
    /// </summary>
    private static Task<bool> ValidateAsync(OrderStatusChangedEvent value, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(value.WarehouseCode))
        {
            throw new InvalidOperationException(
                $"OrderStatusChanged event for OrderId '{value.OrderId}' has a null/empty WarehouseCode - cannot classify or route.");
        }

        if (string.IsNullOrEmpty(OrderStatusChangedRules.ResolveReferenceId(value)))
        {
            throw new InvalidOperationException(
                $"OrderStatusChanged event for WarehouseCode '{value.WarehouseCode}' resolved a null/empty reference id (OrderId/PickingRouteId) - cannot publish a valid tracking request.");
        }

        return Task.FromResult(true);
    }
}
