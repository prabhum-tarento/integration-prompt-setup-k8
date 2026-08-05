using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockOnHandUpdated.Mappers;

/// <summary>
/// Hand-written mapping from the Avro-generated <c>net.pandora.nexus.event.inventory.StockOnHandUpdated</c>
/// SpecificRecord (NexusFacades.Common.AvroSchemas) to this consumer's own decoupled
/// <see cref="StockOnHandUpdatedEvent"/> wire contract - no mapping library, same rationale as
/// <see cref="InventoryStateChangedEventMapper"/>. <c>channel</c>, <c>location</c> and the nested
/// <c>state</c> share the exact same Avro shapes <see cref="InventoryStateChangedEventMapper"/>
/// already maps, so this reuses that mapper's internal <see cref="InventoryStateChangedEventMapper.ToChannel"/>/
/// <see cref="InventoryStateChangedEventMapper.ToLocation"/>/<see cref="InventoryStateChangedEventMapper.ToStateSnapshot"/>
/// helpers rather than duplicating them. Every enum is mapped explicitly by symbol name, not by
/// ordinal - same reasoning as <see cref="InventoryStateChangedEventMapper"/> - except
/// <c>countryOfOrigin</c>, kept as its raw ISO code string, same reasoning as
/// <see cref="InventoryEventItemLine.CountryOfOrigin"/>. The Avro <c>market</c> field is intentionally
/// NOT mapped onto <see cref="StockOnHandUpdatedEvent"/> - see that type's remarks.
/// </summary>
internal static class StockOnHandUpdatedEventMapper
{
    public static StockOnHandUpdatedEvent ToStockOnHandUpdatedEvent(this net.pandora.nexus.@event.inventory.StockOnHandUpdated source) =>
        new(
            InventoryStateChangedEventMapper.ToChannel(source.channel),
            source.referenceId,
            source.updatedDate,
            InventoryStateChangedEventMapper.ToLocation(source.location),
            source.entity,
            source.productId,
            source.productUnits,
            source.barcode,
            source.quantityDetails.Select(ToQuantityDetail).ToArray(),
            ToReason(source.reason));

    private static StockOnHandQuantityDetail ToQuantityDetail(net.pandora.nexus.@event.inventory.AbsoluteQuantityDetail detail) =>
        new(
            detail.quantity,
            InventoryStateChangedEventMapper.ToStateSnapshot(detail.state),
            detail.countryOfOrigin.ToString(),
            detail.hallmarking,
            ToDomain(detail.domain));

    private static StockOnHandInventoryDomain ToDomain(net.pandora.nexus.@object.inventory.InventoryDomain domain) => domain switch
    {
        net.pandora.nexus.@object.inventory.InventoryDomain.B2B => StockOnHandInventoryDomain.B2B,
        net.pandora.nexus.@object.inventory.InventoryDomain.B2C => StockOnHandInventoryDomain.B2C,
        net.pandora.nexus.@object.inventory.InventoryDomain.INTERNAL_HALLMARKING => StockOnHandInventoryDomain.InternalHallmarking,
        net.pandora.nexus.@object.inventory.InventoryDomain.EXTERNAL_HALLMARKING => StockOnHandInventoryDomain.ExternalHallmarking,
        net.pandora.nexus.@object.inventory.InventoryDomain.OMNI => StockOnHandInventoryDomain.Omni,
        _ => StockOnHandInventoryDomain.Unknown,
    };

    private static StockOnHandUpdatedReason ToReason(net.pandora.nexus.@object.inventory.ReasonCode reason) => reason switch
    {
        net.pandora.nexus.@object.inventory.ReasonCode.ADJUSTMENT => StockOnHandUpdatedReason.Adjustment,
        net.pandora.nexus.@object.inventory.ReasonCode.BUNDLING => StockOnHandUpdatedReason.Bundling,
        net.pandora.nexus.@object.inventory.ReasonCode.COUNTING => StockOnHandUpdatedReason.Counting,
        net.pandora.nexus.@object.inventory.ReasonCode.CUSTOMER_RETURN => StockOnHandUpdatedReason.CustomerReturn,
        net.pandora.nexus.@object.inventory.ReasonCode.OTHER => StockOnHandUpdatedReason.Other,
        net.pandora.nexus.@object.inventory.ReasonCode.RECEIPT => StockOnHandUpdatedReason.Receipt,
        net.pandora.nexus.@object.inventory.ReasonCode.RECEIPT_ADJUSTMENT => StockOnHandUpdatedReason.ReceiptAdjustment,
        net.pandora.nexus.@object.inventory.ReasonCode.RETURN => StockOnHandUpdatedReason.Return,
        net.pandora.nexus.@object.inventory.ReasonCode.SALE => StockOnHandUpdatedReason.Sale,
        net.pandora.nexus.@object.inventory.ReasonCode.TRANSFER => StockOnHandUpdatedReason.Transfer,
        net.pandora.nexus.@object.inventory.ReasonCode.VENDOR_RETURN => StockOnHandUpdatedReason.VendorReturn,
        net.pandora.nexus.@object.inventory.ReasonCode.AUTO_RECONCILIATION => StockOnHandUpdatedReason.AutoReconciliation,
        _ => StockOnHandUpdatedReason.Unknown,
    };
}
