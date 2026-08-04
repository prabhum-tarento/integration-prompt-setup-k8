using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockSyncSubmitted.Mappers;

/// <summary>
/// Hand-written mapping from the Avro-generated <c>net.pandora.nexus.event.inventory.StockSyncSubmitted</c>
/// SpecificRecord (NexusFacades.Common.AvroSchemas) to this consumer's own decoupled
/// <see cref="StockSyncSubmittedEvent"/> wire contract - no mapping library, same rationale as
/// <see cref="InventoryStateChangedEventMapper"/>. <c>channel</c> and <c>location</c> share the exact
/// same Avro shapes <see cref="InventoryStateChangedEventMapper"/> already maps
/// (<c>net.pandora.nexus.shared.Channel</c>/<c>Location</c>), so this reuses that mapper's internal
/// <see cref="InventoryStateChangedEventMapper.ToChannel"/>/<see cref="InventoryStateChangedEventMapper.ToLocation"/>
/// helpers rather than duplicating them. Every enum is mapped explicitly by symbol name, not by
/// ordinal - same reasoning as <see cref="InventoryStateChangedEventMapper"/> - except
/// <c>countryOfOrigin</c>, kept as its raw ISO code string, same reasoning as
/// <see cref="InventoryEventItemLine.CountryOfOrigin"/>.
/// </summary>
internal static class StockSyncSubmittedEventMapper
{
    public static StockSyncSubmittedEvent ToStockSyncSubmittedEvent(this net.pandora.nexus.@event.inventory.StockSyncSubmitted source) =>
        new(
            InventoryStateChangedEventMapper.ToChannel(source.channel),
            source.syncDate,
            InventoryStateChangedEventMapper.ToLocation(source.location),
            source.entity,
            source.productId,
            source.productUnits,
            source.quantityDetails.Select(ToQuantityDetail).ToArray());

    private static StockSyncQuantityDetail ToQuantityDetail(net.pandora.nexus.@event.inventory.InventoryQuantityDetail detail) =>
        new(
            detail.quantity,
            InventoryStateChangedEventMapper.ToStateSnapshot(detail.state),
            ToDomain(detail.domain),
            detail.countryOfOrigin.ToString(),
            detail.hallmarking);

    private static StockSyncInventoryDomain ToDomain(net.pandora.nexus.@object.inventory.InventoryDomain domain) => domain switch
    {
        net.pandora.nexus.@object.inventory.InventoryDomain.B2B => StockSyncInventoryDomain.B2B,
        net.pandora.nexus.@object.inventory.InventoryDomain.B2C => StockSyncInventoryDomain.B2C,
        net.pandora.nexus.@object.inventory.InventoryDomain.INTERNAL_HALLMARKING => StockSyncInventoryDomain.InternalHallmarking,
        net.pandora.nexus.@object.inventory.InventoryDomain.EXTERNAL_HALLMARKING => StockSyncInventoryDomain.ExternalHallmarking,
        net.pandora.nexus.@object.inventory.InventoryDomain.OMNI => StockSyncInventoryDomain.Omni,
        _ => StockSyncInventoryDomain.Unknown,
    };
}
