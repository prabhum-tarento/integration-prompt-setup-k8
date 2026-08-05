using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived.Mappers;

/// <summary>
/// Hand-written mapping from the Avro-generated <see cref="net.pandora.nexus.@event.b2b.purchase.GoodsInTransitReceived"/>
/// SpecificRecord (NexusFacades.Common.AvroSchemas) to this consumer's own decoupled
/// <see cref="GoodsInTransitReceivedEvent"/> wire contract - no mapping library, same rationale as
/// <see cref="InventoryStateChangedEventMapper"/>. Every Avro type referenced below is fully qualified
/// rather than <c>using</c>'d. <see cref="InventoryStateChangedEventMapper.ToChannel"/>/
/// <see cref="InventoryStateChangedEventMapper.ToLocation"/> are reused as-is since <c>channel</c>/
/// <c>Location</c> are the identical Avro shared shapes already consumed by <c>InventoryStateChanged</c>.
/// </summary>
internal static class GoodsInTransitReceivedEventMapper
{
    public static GoodsInTransitReceivedEvent ToGoodsInTransitReceivedEvent(
        this net.pandora.nexus.@event.b2b.purchase.GoodsInTransitReceived source) =>
        new(
            InventoryStateChangedEventMapper.ToChannel(source.channel),
            ToShipment(source.shipment));

    private static GoodsInTransitShipment ToShipment(net.pandora.nexus.@object.b2b.purchase.Shipment shipment) =>
        new(
            shipment.packingSlipId,
            shipment.receiptDate,
            shipment.warehouseCode,
            shipment.vendorCode,
            shipment.locationTo is null ? null : InventoryStateChangedEventMapper.ToLocation(shipment.locationTo),
            shipment.shipmentLines.Select(ToShipmentLine).ToArray());

    private static GoodsInTransitShipmentLine ToShipmentLine(net.pandora.nexus.@object.b2b.purchase.ShipmentLine line) =>
        new(
            line.lineNum,
            line.productId,
            line.quantity,
            line.countryOfOrigin?.ToString(),
            line.returnReasonCode,
            line.hallmarking);
}
