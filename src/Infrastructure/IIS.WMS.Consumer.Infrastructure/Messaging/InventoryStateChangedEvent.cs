namespace IIS.WMS.Consumer.Infrastructure.Messaging;

/// <summary>
/// This consumer's own decoupled wire contract for an <c>InventoryStateChanged</c> event -
/// mirrors <c>net.pandora.nexus.event.inventory.InventoryStateChanged</c> (the Avro-generated
/// SpecificRecord from the NexusFacades.Common.AvroSchemas package) field-for-field, but as a plain
/// type with no Avro codegen ties (no <c>Schema</c> property, no <c>ISpecificRecord</c>) - a future
/// Avro schema change only ripples into <c>Kafka.InventoryStateChangedEventMapper</c>, not into the
/// JSON audit trail/Service Bus payload shape this type defines. Mapped from the Avro type by
/// <see cref="Kafka.InventoryStateChangedEventMapper"/> (hand-written, no mapping library - see that
/// class's remarks).
/// </summary>
public sealed record InventoryStateChangedEvent(
    InventoryEventChannel Channel,
    string Id,
    DateTime ChangeDate,
    InventoryEventLocation Location,
    string? Entity,
    InventoryEventChangeType Type,
    InventoryEventStateSnapshot FromState,
    InventoryEventStateSnapshot ToState,
    IReadOnlyList<InventoryEventItemLine> ItemLines,
    string? ReferenceId);

/// <summary>The location where the items are physically located.</summary>
public sealed record InventoryEventLocation(string Id, InventoryEventLocationType Type);

/// <summary>An inventory state/status pair - used for both <see cref="InventoryStateChangedEvent.FromState"/> and <see cref="InventoryStateChangedEvent.ToState"/>.</summary>
public sealed record InventoryEventStateSnapshot(InventoryEventStockState State, InventoryEventStockStatus Status);

/// <summary>One line item within an <see cref="InventoryStateChangedEvent"/>.</summary>
/// <param name="CountryOfOrigin">Raw ISO 3166-1 alpha-2 code string, not a mirrored enum - nothing here branches on it, and it's a ~250-symbol list.</param>
public sealed record InventoryEventItemLine(
    string LineNum,
    string ProductId,
    string? ItemName,
    int Quantity,
    string? Units,
    string CountryOfOrigin,
    string Hallmarking,
    InventoryEventWeight? NetWeight,
    InventoryEventWeight? TareWeight,
    InventoryEventMoney? UnitPrice,
    string? CommodityCode,
    string? ItemCategoryLocalized,
    string? ItemMaterialNameLocalized,
    string? InventoryRegistrationId,
    string? CustomsRegistrationLineNum,
    bool? IsBonded);

/// <summary>A weight measurement.</summary>
public sealed record InventoryEventWeight(InventoryEventWeightUnit Unit, int Quantity);

/// <summary>
/// An amount of money - <see cref="Amount"/> combines the Avro <c>Money</c> type's <c>units</c>
/// (whole units) and <c>nanos</c> (fractional, 10^-9) fields into one <see cref="decimal"/>, per
/// dotnet-architecture-good-practices.instructions.md §5's "monetary fields use decimal" rule.
/// </summary>
/// <param name="CurrencyCode">Raw ISO 4217 code string, not a mirrored enum - same reasoning as <see cref="InventoryEventItemLine.CountryOfOrigin"/>.</param>
public sealed record InventoryEventMoney(string CurrencyCode, decimal Amount);

public enum InventoryEventChannel
{
    Unknown,
    OwnPhysical,
    OwnOnline,
    PartnerOnline,
    Franchise,
    Distributor,
    Wholesale,
    OtherStores,
    Production,
    Employee,
    Marketing,
    Procurement,
    Finance,
    IntercompanyDistribution,
    WholesaleEdi,
}

public enum InventoryEventLocationType
{
    Unknown,
    Warehouse,
    Store,
    DarkStore,
    Transit,
    ProductionSite,
    Zone,
    ThirdPartyLogistics,
    Digital,
    WarehouseGroup,
}

public enum InventoryEventChangeType
{
    Unknown,
    Blc,
    Cie,
    Cin,
    Cmd,
    Ent,
    Mav,
    Min,
    Mrp,
    Mpr,
    Mqa,
    Mqp,
    Oia,
    Rfr,
    Rrt,
    Rtr,
    PickedB2B,
    PickedB2C,
    Dgp,
}

public enum InventoryEventStockState
{
    Unknown,
    Available,
    Blocked,
    Inspection,
    Scrap,
    Rework,
    Remelt,
    Stone,
    AvailableToSell,
}

public enum InventoryEventStockStatus
{
    Unknown,
    Pickable,
    Held,
    Prepared,
    Hallmarking,
    Allocated,
    Invoiced,
}

public enum InventoryEventWeightUnit
{
    Unknown,
    Milligram,
    Gram,
    Kilogram,
    Carat,
}
