using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived;

/// <summary>
/// This consumer's own decoupled wire contract for a <c>GoodsInTransitReceived</c> event - mirrors
/// <c>net.pandora.nexus.event.b2b.purchase.GoodsInTransitReceived</c> (the Avro-generated SpecificRecord
/// from the NexusFacades.Common.AvroSchemas package) field-for-field, but only the subset §3
/// (docs/events/b2b.purchase.GoodsInTransitReceived.md) actually consumes - same rationale as
/// <see cref="OrderStatusChanged.OrderStatusChangedEvent"/>. Reuses <see cref="InventoryEventChannel"/>/
/// <see cref="InventoryEventLocation"/> since the underlying Avro <c>channel</c>/<c>Location</c> shapes are
/// identical to the ones <c>InventoryStateChanged</c> already consumes.
/// </summary>
public sealed record GoodsInTransitReceivedEvent(
    InventoryEventChannel Channel,
    GoodsInTransitShipment Shipment);

/// <summary>
/// The wire subset of the Avro <c>Shipment</c> record (<c>net.pandora.nexus.object.b2b.purchase.Shipment</c>).
/// </summary>
public sealed record GoodsInTransitShipment(
    string PackingSlipId,
    DateTime ReceiptDate,
    string WarehouseCode,
    string VendorCode,
    InventoryEventLocation? LocationTo,
    IReadOnlyList<GoodsInTransitShipmentLine> ShipmentLines);

/// <summary>
/// The wire subset of the Avro <c>ShipmentLine</c> record.
/// </summary>
/// <remarks>
/// TODO(ai): unresolved precedence conflict - docs/events/b2b.purchase.GoodsInTransitReceived.md §7.1's
/// simplified pseudocode shows <c>LineNum</c> as a required non-nullable string and omits
/// <c>Hallmarking</c> entirely, while the real Avro schema (scripts/local-kafka/registration/events/
/// b2b-purchase-order-events/b2b.purchase.GoodsInTransitReceived/schema.avsc) has <c>lineNum</c> as
/// nullable and does include <c>hallmarking</c> (nullable). This DTO follows the real Avro schema (the
/// authoritative wire contract) rather than the doc's pseudocode; review before shipping.
/// </remarks>
public sealed record GoodsInTransitShipmentLine(
    string? LineNum,
    string ProductId,
    int Quantity,
    string? CountryOfOrigin,
    string? ReturnReasonCode,
    string? Hallmarking);
