using IIS.WMS.Consumer.Application.OrderTracking;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged.Handlers;

/// <summary>
/// Applies one relayed <see cref="OrderStatusChangedEvent"/>
/// (docs/events/b2b.sales.OrderStatusChanged.md §2/§3) - warehouse classification, reference-id
/// selection, status mapping, and fulfilment-unit-id normalization (all via
/// <see cref="OrderStatusChangedRules"/>, shared with <see cref="OrderStatusChangedConsumerHostedService"/>'s
/// own Kafka-side validation/routing so both layers resolve the same reference id), followed by a
/// single, unconditional order-tracking publish - unlike
/// <see cref="InventoryStateChanged.Handlers.InventoryStateChangedHandler"/>, which only publishes on a
/// pick/unpick transition, every message here publishes exactly one tracking request (doc §2 step 9/§6).
/// This event carries no Cosmos DB access (doc §5) and no item lines (doc §7/§10 - <c>Lines</c> is
/// always published empty), so there is no per-item-line fan-out and no OMS-delta/ICR-snapshot publish
/// to gate.
/// </summary>
/// <param name="orderTrackingPublisher">§7/§8 order-tracking publisher - failures are logged and swallowed by the shared implementation, same as every other caller (docs/events/b2b.sales.OrderStatusChanged.md §8 literally calls a publish failure a processing failure/DeadLetter, which this shared publisher does not do - see the implementation summary for this documented divergence).</param>
/// <param name="logger">Logger for the tracking-request publish. Never logs <see cref="OrderStatusChangedEvent.CancelReason"/> or <see cref="OrderStatusChangedEvent.SourceOrderReferenceId"/> - both are flagged sensitive (doc §9) and neither is otherwise used by this handler.</param>
public sealed class OrderStatusChangedHandler(
    IOrderTrackingPublisher orderTrackingPublisher,
    ILogger<OrderStatusChangedHandler> logger)
    : IOrderStatusChangedHandler
{
    /// <inheritdoc/>
    public async Task HandleAsync(OrderStatusChangedEvent message, string correlationId, CancellationToken cancellationToken)
    {
        var referenceId = OrderStatusChangedRules.ResolveReferenceId(message);

        if (string.IsNullOrEmpty(referenceId))
        {
            logger.LogWarning(
                "OrderStatusChanged event for WarehouseCode {WarehouseCode}, CorrelationId {CorrelationId} resolved an empty reference id - skipping order-tracking publish.",
                message.WarehouseCode, correlationId);

            return;
        }

        var request = new OrderTrackingRelayRequest(
            ReferenceId: referenceId,
            Channel: message.Channel.ToString(),
            FulfilmentUnitId: OrderStatusChangedRules.NormalizeFulfilmentUnitId(message.WarehouseCode),
            FulfilmentUnitType: InventoryEventLocationType.Warehouse.ToString(),
            FunctionName: nameof(OrderStatusChangedHandler),
            OrderId: referenceId,
            OrderStatus: ToOrderTrackingStatus(message.Status),
            OrderType: OrderType.SALES.ToString(),
            Lines: []);

        await orderTrackingPublisher.PublishAsync(request, cancellationToken);
    }

    /// <summary>§3.3 - only CANCELLED/DELETED are meaningful; every other <see cref="OrderStatusCode"/> (including <see cref="OrderStatusCode.OrderCanceled"/>, intentionally distinct from <see cref="OrderStatusCode.Cancelled"/>) maps to UNKNOWN by design.</summary>
    private static OrderTrackingStatus ToOrderTrackingStatus(OrderStatusCode status) => status switch
    {
        OrderStatusCode.Cancelled => OrderTrackingStatus.CANCELLED,
        OrderStatusCode.Deleted => OrderTrackingStatus.DELETED,
        _ => OrderTrackingStatus.UNKNOWN,
    };
}
