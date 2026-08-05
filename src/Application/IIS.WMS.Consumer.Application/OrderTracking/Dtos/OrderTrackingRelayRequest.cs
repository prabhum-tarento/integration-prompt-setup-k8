namespace IIS.WMS.Consumer.Application.OrderTracking.Dtos;

/// <summary>
/// The OrderTracking relay request built for a pick/unpick <c>InventoryStateChangedEvent</c> -
/// ported from the upstream Reflex facade's <c>OrderTrackingCommonOrchestratorRequest</c>
/// (<c>IIS.WMS.Reflex.Domain.Events.OrderTracking</c>), narrowed to only the fields
/// <c>InventoryStateChangedQueueTrigger</c> actually populates.
/// </summary>
/// <param name="ReferenceId">The inventory event's own id.</param>
/// <param name="Channel">The sales channel the transition occurred on.</param>
/// <param name="FulfilmentUnitId">The fulfilment location id.</param>
/// <param name="FulfilmentUnitType">The fulfilment location type.</param>
/// <param name="FunctionName">The name of the component that built this request, for traceability.</param>
/// <param name="OrderId">The order/reference id this transition relates to, or <see langword="null"/> if none.</param>
/// <param name="OrderStatus">Always <c>"PICKED"</c> - the only status this relay produces.</param>
/// <param name="OrderType">Either <c>"SALES"</c> or <c>"TRANSFER"</c>, mirroring Reflex's own <c>OrderType.ToString()</c>.</param>
/// <param name="Lines">The item lines carried by the source event.</param>
/// <param name="Type">Event-type discriminator (e.g. <c>"B2B_CONSOLIDATED_ORDER_SHIPPED"</c>) - optional, only populated by callers that need to distinguish it downstream (docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.2).</param>
/// <param name="PackingSlipId">The packing slip id resolved per §3.2's warehouse-specific assignment rule, or <see langword="null"/> for callers that don't carry one.</param>
/// <param name="ShipmentId">The originating shipment id, or <see langword="null"/> for callers that don't carry one.</param>
/// <param name="ShipDate">The shipment date, or <see langword="null"/> for callers that don't carry one.</param>
/// <param name="Market">The order's market, or <see langword="null"/> if not applicable/known.</param>
/// <param name="IsExport">Whether the shipment is an export shipment - only meaningful alongside <see cref="Type"/>.</param>
public sealed record OrderTrackingRelayRequest(
    string ReferenceId,
    string Channel,
    string FulfilmentUnitId,
    string FulfilmentUnitType,
    string FunctionName,
    string? OrderId,
    OrderTrackingStatus OrderStatus,
    string OrderType,
    IReadOnlyList<OrderTrackingRelayLine> Lines,
    string? Type = null,
    string? PackingSlipId = null,
    string? ShipmentId = null,
    DateTime? ShipDate = null,
    string? Market = null,
    bool IsExport = false);

/// <summary>One item line within an <see cref="OrderTrackingRelayRequest"/>.</summary>
/// <param name="ItemCode">The product id.</param>
/// <param name="CountryOfOrigin">The line's country of origin.</param>
/// <param name="HallMarkType">The line's hallmarking value.</param>
/// <param name="Qty">The line quantity.</param>
/// <param name="ShipmentLineNumber">The shipment line's lot id (docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.2 <c>LotId</c>), or <see langword="null"/> for callers that don't carry one.</param>
public sealed record OrderTrackingRelayLine(
    string ItemCode,
    string CountryOfOrigin,
    string HallMarkType,
    int Qty,
    string? ShipmentLineNumber = null);

public enum OrderTrackingStatus
{
    UNKNOWN,
    CREATED,
    ALLOCATED,
    PARTIALLYALLOCATED,
    PICKED,
    PARTIALLYPICKED,
    SHIPPED,
    PARTIALLYSHIPPED,
    INTRANSIT,
    DELETED,
    CANCELLED,
    RECEIVED,
    PARTIALLYRECEIVED,
    DELIVERED,
    PARTIALLYDELIVERED,
    INVOICED,
    PARTIALLYINVOICED,
    PARTIALLYCANCELLED,
}

