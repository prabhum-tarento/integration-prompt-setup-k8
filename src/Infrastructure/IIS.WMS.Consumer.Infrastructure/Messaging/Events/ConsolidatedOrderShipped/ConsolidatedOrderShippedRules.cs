using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped;

/// <summary>
/// Warehouse classification, grouping-key resolution, packing-slip-id assignment, and order-status/
/// eligibility rules (docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.2) - shared between
/// <see cref="ConsolidatedOrderShippedConsumerHostedService"/>'s Kafka-side <c>getServiceBusRouting</c>
/// delegate and <see cref="Handlers.ConsolidatedOrderShippedHandler"/> (which needs the same
/// classification again to build order-tracking requests), so the two layers can never disagree.
/// </summary>
/// <remarks>
/// TODO(ai): unresolved precedence conflict - §3.2's grouping pseudocode ("Lines are grouped by OrderId
/// for 3PL, by PickingRouteId for TDC/ADC", §1 Assumption 6) reads as if OrderId/PickingRouteId are
/// shipment-level, but the real Avro schema has both fields on each <see cref="ConsolidatedOrderShipmentLine"/>,
/// not on <see cref="ConsolidatedOrderShipment"/> (see <see cref="ConsolidatedOrderShippedEvent"/>'s own
/// TODO(ai)). <see cref="ResolveGroupKey"/> therefore resolves the key per line rather than per shipment -
/// review before shipping.
/// </remarks>
internal static class ConsolidatedOrderShippedRules
{
    /// <summary>§3.2 - true for every warehouse except the three TDC/ADC identifiers, compared case-insensitively.</summary>
    public static bool IsNotTdcOrAdc(string warehouseCode) =>
        !ConsolidatedOrderShippedWarehouseIds.SpecialWarehouses.Contains(warehouseCode);

    /// <summary>
    /// §3.2 - <c>OrderId</c> for 3PL warehouses, <c>PickingRouteId</c> for TDC/ADC, resolved per shipment
    /// line since both fields live on <see cref="ConsolidatedOrderShipmentLine"/> in the real Avro schema.
    /// </summary>
    public static string ResolveGroupKey(string warehouseCode, ConsolidatedOrderShipmentLine line) =>
        IsNotTdcOrAdc(warehouseCode) ? line.OrderId : line.PickingRouteId;

    /// <summary>
    /// §3.2 - TDC (either its SAP id or its fulfilment id, compared case-insensitively per §4) uses the
    /// parent order id as the packing slip id; every other warehouse uses the shipment's own.
    /// </summary>
    public static string ResolvePackingSlipId(string warehouseCode, string parentOrderId, string shipmentPackingSlipId) =>
        string.Equals(warehouseCode, ConsolidatedOrderShippedWarehouseIds.Tdc, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(warehouseCode, ConsolidatedOrderShippedWarehouseIds.TdcSapId, StringComparison.OrdinalIgnoreCase)
            ? parentOrderId
            : shipmentPackingSlipId;

    /// <summary>§3.2 - default SHIPPED; a preliminary export confirmation is reported as INVOICED instead.</summary>
    public static OrderTrackingStatus ResolveOrderStatus(ConfirmationType confirmationType, bool isExport) =>
        confirmationType == ConfirmationType.PRELIMINARY && isExport ? OrderTrackingStatus.INVOICED : OrderTrackingStatus.SHIPPED;

    /// <summary>§3.2 - preliminary confirmations are only order-tracking-eligible when the shipment is an export.</summary>
    public static bool IsOrderTrackingEligible(ConfirmationType confirmationType, bool isExport) =>
        confirmationType != ConfirmationType.PRELIMINARY || isExport;
}
