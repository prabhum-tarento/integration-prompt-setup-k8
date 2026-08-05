using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped.Mappers;

/// <summary>
/// Hand-written mapping from the Avro-generated
/// <see cref="net.pandora.nexus.@event.b2b.sales.ConsolidatedOrderShipped"/> SpecificRecord
/// (NexusFacades.Common.AvroSchemas) to this consumer's own decoupled
/// <see cref="ConsolidatedOrderShippedEvent"/> wire contract - no mapping library, same rationale as
/// <see cref="InventoryStateChangedEventMapper"/>. Every Avro type referenced below is fully qualified
/// rather than <c>using</c>'d. <c>Shipment</c>, <c>ShipmentLine</c>, and the nested <c>ConfirmationType</c>
/// enum declare no explicit Avro <c>namespace</c>, so per Avro's namespace-inheritance rule they inherit
/// the enclosing record's namespace - expected codegen types are
/// <see cref="net.pandora.nexus.@event.b2b.sales.Shipment"/>,
/// <see cref="net.pandora.nexus.@event.b2b.sales.ShipmentLine"/>, and
/// <see cref="net.pandora.nexus.@event.b2b.sales.ConfirmationType"/> - confirm at build time.
/// <see cref="InventoryStateChangedEventMapper.ToChannel"/> is reused as-is since <c>channel</c> is the
/// identical Avro shared shape already consumed by <c>InventoryStateChanged</c>. The Domain-owned
/// <see cref="Domain.Enums.ConfirmationType"/> enum is a direct passthrough of the Avro enum - symbols
/// match 1:1 (see that enum's own doc comment).
/// </summary>
internal static class ConsolidatedOrderShippedEventMapper
{
    public static ConsolidatedOrderShippedEvent ToConsolidatedOrderShippedEvent(
        this net.pandora.nexus.@event.b2b.sales.ConsolidatedOrderShipped source) =>
        new(
            InventoryStateChangedEventMapper.ToChannel(source.channel),
            source.market?.ToString(),
            source.parentOrderId,
            ToShipment(source.shipment),
            source.isExport);

    private static ConsolidatedOrderShipment ToShipment(net.pandora.nexus.@event.b2b.sales.Shipment shipment) =>
        new(
            shipment.id,
            shipment.warehouseCode,
            ToConfirmationType(shipment.confirmationType),
            shipment.shipDate,
            shipment.packingSlipId,
            shipment.shipmentLines.Select(ToShipmentLine).ToArray());

    private static ConsolidatedOrderShipmentLine ToShipmentLine(net.pandora.nexus.@event.b2b.sales.ShipmentLine line) =>
        new(
            line.lineNum,
            line.pickingRouteId,
            line.orderId,
            line.lotId,
            line.productId,
            line.quantity,
            line.hallmarking,
            line.allocatedFromB2BBucketQuantity,
            line.countryOfOrigin?.ToString());

    private static Domain.Enums.ConfirmationType ToConfirmationType(net.pandora.nexus.@event.b2b.sales.ConfirmationType confirmationType) => confirmationType switch
    {
        net.pandora.nexus.@event.b2b.sales.ConfirmationType.PRELIMINARY => Domain.Enums.ConfirmationType.PRELIMINARY,
        net.pandora.nexus.@event.b2b.sales.ConfirmationType.STANDARD => Domain.Enums.ConfirmationType.STANDARD,
        net.pandora.nexus.@event.b2b.sales.ConfirmationType.STANDARD_FOLLOWING_PRELIMINARY => Domain.Enums.ConfirmationType.STANDARD_FOLLOWING_PRELIMINARY,
        net.pandora.nexus.@event.b2b.sales.ConfirmationType.PRELIMINARY_INVOICE => Domain.Enums.ConfirmationType.PRELIMINARY_INVOICE,
        net.pandora.nexus.@event.b2b.sales.ConfirmationType.PRELIMINARY_EXPORT => Domain.Enums.ConfirmationType.PRELIMINARY_EXPORT,
        _ => Domain.Enums.ConfirmationType.UNKNOWN,
    };
}
