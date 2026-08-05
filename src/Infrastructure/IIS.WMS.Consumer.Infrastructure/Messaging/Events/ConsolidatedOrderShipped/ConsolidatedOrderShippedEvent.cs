using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped;

/// <summary>
/// This consumer's own decoupled wire contract for a <c>ConsolidatedOrderShipped</c> event - mirrors
/// <c>net.pandora.nexus.event.b2b.sales.ConsolidatedOrderShipped</c> (the Avro-generated SpecificRecord
/// from the NexusFacades.Common.AvroSchemas package) field-for-field, but only the subset §3
/// (docs/events/b2b.sales.ConsolidatedOrderShipped.md) actually consumes - same rationale as
/// <see cref="OrderStatusChanged.OrderStatusChangedEvent"/>. Reuses <see cref="InventoryEventChannel"/> for
/// <see cref="Channel"/> and the Domain-owned <see cref="ConfirmationType"/> directly for
/// <see cref="ConsolidatedOrderShipment.ConfirmationType"/> (symbols match the Avro schema 1:1 - see
/// <see cref="ConfirmationType"/>'s own doc comment). <c>OrderCharges</c> and
/// <c>MasterShipmentTrackingId</c> are intentionally omitted - unused by this event's §3 logic.
/// </summary>
/// <remarks>
/// TODO(ai): unresolved precedence conflict - docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.2's
/// grouping pseudocode ("Lines are grouped by OrderId for 3PL, by PickingRouteId for TDC/ADC", §1
/// Assumption 6) reads as if OrderId/PickingRouteId are shipment-level, but the real Avro schema
/// (scripts/local-kafka/registration/events/b2b-sales-order-events/b2b.sales.ConsolidatedOrderShipped/schema.avsc)
/// has both fields on each <see cref="ConsolidatedOrderShipmentLine"/>, not on <see cref="ConsolidatedOrderShipment"/>.
/// This DTO follows the real Avro schema (the authoritative wire contract); the Handler groups per-line
/// by the resolved key rather than treating the whole shipment as one group. Review before shipping.
/// </remarks>
public sealed record ConsolidatedOrderShippedEvent(
    InventoryEventChannel Channel,
    string? Market,
    string ParentOrderId,
    ConsolidatedOrderShipment Shipment,
    bool IsExport);

/// <summary>The wire subset of the Avro <c>Shipment</c> record.</summary>
public sealed record ConsolidatedOrderShipment(
    string Id,
    string WarehouseCode,
    ConfirmationType ConfirmationType,
    DateTime ShipDate,
    string PackingSlipId,
    IReadOnlyList<ConsolidatedOrderShipmentLine> ShipmentLines);

/// <summary>The wire subset of the Avro <c>ShipmentLine</c> record.</summary>
public sealed record ConsolidatedOrderShipmentLine(
    string LineNum,
    string PickingRouteId,
    string OrderId,
    string LotId,
    string ProductId,
    int Quantity,
    string? Hallmarking,
    int? AllocatedFromB2BBucketQuantity,
    string? CountryOfOrigin);
