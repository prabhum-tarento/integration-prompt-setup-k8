namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged;

/// <summary>
/// Warehouse classification, reference-id selection, and fulfilment-unit-id normalization
/// (docs/events/b2b.sales.OrderStatusChanged.md §3.1/§3.2/§3.4) - shared between
/// <see cref="OrderStatusChangedConsumerHostedService"/>'s Kafka-side <c>validateAsync</c>/<c>getServiceBusRouting</c>
/// delegates (which need the resolved reference id before relaying) and
/// <see cref="Handlers.OrderStatusChangedHandler"/> (which needs it again to build the order-tracking
/// request), so the two layers can never disagree on which id a given message resolves to.
/// </summary>
internal static class OrderStatusChangedRules
{
    /// <summary>§3.1 - true for every warehouse except the three TDC/ADC identifiers, compared case-insensitively (§4).</summary>
    public static bool IsNotTdcOrAdc(string warehouseCode) =>
        !OrderStatusChangedWarehouseIds.SpecialWarehouses.Contains(warehouseCode);

    /// <summary>§3.2 - <c>OrderId</c> for standard warehouses, <c>PickingRouteId</c> for TDC/ADC. May be null/empty if the source event omitted it - callers must reject that case rather than publish an invalid reference.</summary>
    public static string? ResolveReferenceId(OrderStatusChangedEvent message) =>
        IsNotTdcOrAdc(message.WarehouseCode) ? message.OrderId : message.PickingRouteId;

    /// <summary>§3.4 - the TDC SAP id normalizes to its fulfilment id; every other warehouse code (including ADC) passes through unchanged.</summary>
    public static string NormalizeFulfilmentUnitId(string warehouseCode) =>
        warehouseCode == OrderStatusChangedWarehouseIds.TdcSapId ? OrderStatusChangedWarehouseIds.Tdc : warehouseCode;
}
