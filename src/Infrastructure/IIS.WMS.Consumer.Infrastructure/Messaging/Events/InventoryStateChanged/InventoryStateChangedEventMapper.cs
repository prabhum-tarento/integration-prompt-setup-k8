namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

/// <summary>
/// Hand-written mapping from the Avro-generated <see cref="net.pandora.nexus.@event.inventory.InventoryStateChanged"/> SpecificRecord
/// (NexusFacades.Common.AvroSchemas) to this consumer's own decoupled
/// <see cref="InventoryStateChangedEvent"/> wire contract - no mapping library: AutoMapper was
/// evaluated and rejected for this (last MIT-licensed version 14.0.0 carries an unpatched
/// high-severity DoS, GHSA-rvv3-g6hj-g44x/CVE-2026-32933; the fix landed only in the
/// commercially-licensed 15.1.1+/16.1.1+), and every other consumer in this repo already maps by
/// hand (e.g. <c>BulkInventoryImportEvent</c>). Every Avro type referenced below is fully qualified
/// rather than <c>using</c>'d - several simple names here (<c>Status</c>, <c>State</c>,
/// <c>InventoryChangeType</c>, <c>ItemLine</c>) exist in more than one
/// <c>net.pandora.nexus.*</c> namespace, so an unqualified <c>using</c> would be ambiguous or
/// silently bind to the wrong one. Every enum is mapped explicitly by symbol name, not by ordinal -
/// Avro symbol order isn't guaranteed stable across schema evolution - except
/// <c>CountryCode</c>/<c>CurrencyCode</c>, kept as their raw ISO code string
/// (<see cref="InventoryEventItemLine.CountryOfOrigin"/>/<see cref="InventoryEventMoney.CurrencyCode"/>)
/// since nothing here branches on them and mirroring their ~150-250-symbol enums by hand isn't worth
/// it for a value that's never more than logged/relayed as-is. <see cref="ToChannel"/>,
/// <see cref="ToChangeType"/>, <see cref="ToStateSnapshot"/>, <see cref="ToLocation"/>, and
/// <see cref="ToItemLine"/> are <see langword="internal"/> rather than <see langword="private"/> so
/// <see cref="InventoryAdjustedEventMapper"/> can reuse them for the identical Avro shapes shared
/// between <c>InventoryStateChanged</c> and <c>InventoryAdjusted</c>.
/// </summary>
internal static class InventoryStateChangedEventMapper
{
    public static InventoryStateChangedEvent ToInventoryStateChangedEvent(this net.pandora.nexus.@event.inventory.InventoryStateChanged source) =>
        new(
            ToChannel(source.channel),
            source.id,
            source.changeDate,
            ToLocation(source.location),
            source.entity,
            ToChangeType(source.type),
            ToStateSnapshot(source.fromState),
            ToStateSnapshot(source.toState),
            source.itemLines.Select(ToItemLine).ToArray(),
            source.referenceId);

    internal static InventoryEventLocation ToLocation(net.pandora.nexus.shared.Location location) =>
        new(location.id, ToLocationType(location.type));

    internal static InventoryEventStateSnapshot ToStateSnapshot(net.pandora.nexus.@object.inventory.InventoryState state) =>
        new(ToStockState(state.state), ToStockStatus(state.status));

    internal static InventoryEventItemLine ToItemLine(net.pandora.nexus.@object.inventory.ItemLine line) =>
        new(
            line.lineNum,
            line.productId,
            line.itemName,
            line.quantity,
            line.units,
            line.countryOfOrigin.ToString(),
            line.hallmarking,
            ToWeight(line.netWeight),
            ToWeight(line.tareWeight),
            ToMoney(line.unitPrice),
            line.commodityCode,
            line.itemCategoryLocalized,
            line.itemMaterialNameLocalized,
            line.inventoryRegistrationId,
            line.customsRegistrationLineNum,
            line.isBonded);

    private static InventoryEventWeight? ToWeight(net.pandora.nexus.shared.Weight? weight) =>
        weight is null ? null : new InventoryEventWeight(ToWeightUnit(weight.unit), weight.quantity);

    private static InventoryEventMoney? ToMoney(net.pandora.nexus.shared.Money? money) =>
        money is null ? null : new InventoryEventMoney(money.currencyCode.ToString(), money.units + (money.nanos / 1_000_000_000m));

    internal static InventoryEventChannel ToChannel(net.pandora.nexus.shared.Channel channel) => channel switch
    {
        net.pandora.nexus.shared.Channel.OWN_PHYSICAL => InventoryEventChannel.OwnPhysical,
        net.pandora.nexus.shared.Channel.OWN_ONLINE => InventoryEventChannel.OwnOnline,
        net.pandora.nexus.shared.Channel.PARTNER_ONLINE => InventoryEventChannel.PartnerOnline,
        net.pandora.nexus.shared.Channel.FRANCHISE => InventoryEventChannel.Franchise,
        net.pandora.nexus.shared.Channel.DISTRIBUTOR => InventoryEventChannel.Distributor,
        net.pandora.nexus.shared.Channel.WHOLESALE => InventoryEventChannel.Wholesale,
        net.pandora.nexus.shared.Channel.OTHER_STORES => InventoryEventChannel.OtherStores,
        net.pandora.nexus.shared.Channel.PRODUCTION => InventoryEventChannel.Production,
        net.pandora.nexus.shared.Channel.EMPLOYEE => InventoryEventChannel.Employee,
        net.pandora.nexus.shared.Channel.MARKETING => InventoryEventChannel.Marketing,
        net.pandora.nexus.shared.Channel.PROCUREMENT => InventoryEventChannel.Procurement,
        net.pandora.nexus.shared.Channel.FINANCE => InventoryEventChannel.Finance,
        net.pandora.nexus.shared.Channel.INTERCOMPANY_DISTRIBUTION => InventoryEventChannel.IntercompanyDistribution,
        net.pandora.nexus.shared.Channel.WHOLESALE_EDI => InventoryEventChannel.WholesaleEdi,
        _ => InventoryEventChannel.Unknown,
    };

    private static InventoryEventLocationType ToLocationType(net.pandora.nexus.shared.LocationType type) => type switch
    {
        net.pandora.nexus.shared.LocationType.WAREHOUSE => InventoryEventLocationType.Warehouse,
        net.pandora.nexus.shared.LocationType.STORE => InventoryEventLocationType.Store,
        net.pandora.nexus.shared.LocationType.DARK_STORE => InventoryEventLocationType.DarkStore,
        net.pandora.nexus.shared.LocationType.TRANSIT => InventoryEventLocationType.Transit,
        net.pandora.nexus.shared.LocationType.PRODUCTION_SITE => InventoryEventLocationType.ProductionSite,
        net.pandora.nexus.shared.LocationType.ZONE => InventoryEventLocationType.Zone,
        net.pandora.nexus.shared.LocationType.THIRD_PARTY_LOGISTICS => InventoryEventLocationType.ThirdPartyLogistics,
        net.pandora.nexus.shared.LocationType.DIGITAL => InventoryEventLocationType.Digital,
        net.pandora.nexus.shared.LocationType.WAREHOUSE_GROUP => InventoryEventLocationType.WarehouseGroup,
        _ => InventoryEventLocationType.Unknown,
    };

    internal static InventoryEventChangeType ToChangeType(net.pandora.nexus.@object.inventory.InventoryChangeType type) => type switch
    {
        net.pandora.nexus.@object.inventory.InventoryChangeType.BLC => InventoryEventChangeType.Blc,
        net.pandora.nexus.@object.inventory.InventoryChangeType.CIE => InventoryEventChangeType.Cie,
        net.pandora.nexus.@object.inventory.InventoryChangeType.CIN => InventoryEventChangeType.Cin,
        net.pandora.nexus.@object.inventory.InventoryChangeType.CMD => InventoryEventChangeType.Cmd,
        net.pandora.nexus.@object.inventory.InventoryChangeType.ENT => InventoryEventChangeType.Ent,
        net.pandora.nexus.@object.inventory.InventoryChangeType.MAV => InventoryEventChangeType.Mav,
        net.pandora.nexus.@object.inventory.InventoryChangeType.MIN => InventoryEventChangeType.Min,
        net.pandora.nexus.@object.inventory.InventoryChangeType.MRP => InventoryEventChangeType.Mrp,
        net.pandora.nexus.@object.inventory.InventoryChangeType.MPR => InventoryEventChangeType.Mpr,
        net.pandora.nexus.@object.inventory.InventoryChangeType.MQA => InventoryEventChangeType.Mqa,
        net.pandora.nexus.@object.inventory.InventoryChangeType.MQP => InventoryEventChangeType.Mqp,
        net.pandora.nexus.@object.inventory.InventoryChangeType.OIA => InventoryEventChangeType.Oia,
        net.pandora.nexus.@object.inventory.InventoryChangeType.RFR => InventoryEventChangeType.Rfr,
        net.pandora.nexus.@object.inventory.InventoryChangeType.RRT => InventoryEventChangeType.Rrt,
        net.pandora.nexus.@object.inventory.InventoryChangeType.RTR => InventoryEventChangeType.Rtr,
        net.pandora.nexus.@object.inventory.InventoryChangeType.PICKEDB2B => InventoryEventChangeType.PickedB2B,
        net.pandora.nexus.@object.inventory.InventoryChangeType.PICKEDB2C => InventoryEventChangeType.PickedB2C,
        net.pandora.nexus.@object.inventory.InventoryChangeType.DGP => InventoryEventChangeType.Dgp,
        _ => InventoryEventChangeType.Unknown,
    };

    private static InventoryEventStockState ToStockState(net.pandora.nexus.@object.inventory.State state) => state switch
    {
        net.pandora.nexus.@object.inventory.State.AVAILABLE => InventoryEventStockState.Available,
        net.pandora.nexus.@object.inventory.State.BLOCKED => InventoryEventStockState.Blocked,
        net.pandora.nexus.@object.inventory.State.INSPECTION => InventoryEventStockState.Inspection,
        net.pandora.nexus.@object.inventory.State.SCRAP => InventoryEventStockState.Scrap,
        net.pandora.nexus.@object.inventory.State.REWORK => InventoryEventStockState.Rework,
        net.pandora.nexus.@object.inventory.State.REMELT => InventoryEventStockState.Remelt,
        net.pandora.nexus.@object.inventory.State.STONE => InventoryEventStockState.Stone,
        net.pandora.nexus.@object.inventory.State.AVAILABLETOSELL => InventoryEventStockState.AvailableToSell,
        _ => InventoryEventStockState.Unknown,
    };

    private static InventoryEventStockStatus ToStockStatus(net.pandora.nexus.@object.inventory.Status status) => status switch
    {
        net.pandora.nexus.@object.inventory.Status.PICKABLE => InventoryEventStockStatus.Pickable,
        net.pandora.nexus.@object.inventory.Status.HELD => InventoryEventStockStatus.Held,
        net.pandora.nexus.@object.inventory.Status.PREPARED => InventoryEventStockStatus.Prepared,
        net.pandora.nexus.@object.inventory.Status.HALLMARKING => InventoryEventStockStatus.Hallmarking,
        net.pandora.nexus.@object.inventory.Status.ALLOCATED => InventoryEventStockStatus.Allocated,
        net.pandora.nexus.@object.inventory.Status.INVOICED => InventoryEventStockStatus.Invoiced,
        _ => InventoryEventStockStatus.Unknown,
    };

    private static InventoryEventWeightUnit ToWeightUnit(net.pandora.nexus.shared.WeightUnit unit) => unit switch
    {
        net.pandora.nexus.shared.WeightUnit.MILLIGRAM => InventoryEventWeightUnit.Milligram,
        net.pandora.nexus.shared.WeightUnit.GRAM => InventoryEventWeightUnit.Gram,
        net.pandora.nexus.shared.WeightUnit.KILOGRAM => InventoryEventWeightUnit.Kilogram,
        net.pandora.nexus.shared.WeightUnit.CARAT => InventoryEventWeightUnit.Carat,
        _ => InventoryEventWeightUnit.Unknown,
    };
}
