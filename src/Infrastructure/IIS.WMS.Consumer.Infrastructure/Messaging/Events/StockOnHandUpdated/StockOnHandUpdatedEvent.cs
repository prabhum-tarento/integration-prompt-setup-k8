using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockOnHandUpdated;

/// <summary>
/// This consumer's own decoupled wire contract for a <c>StockOnHandUpdated</c> event - mirrors
/// <c>net.pandora.nexus.event.inventory.StockOnHandUpdated</c> (the Avro-generated SpecificRecord from
/// the NexusFacades.Common.AvroSchemas package) field-for-field, but as a plain type with no Avro
/// codegen ties, same rationale as <see cref="InventoryStateChangedEvent"/>. Reuses that type's
/// <see cref="InventoryEventChannel"/> and <see cref="InventoryEventLocation"/> since both Avro events
/// share those exact shapes. The wire's <c>market</c> field is intentionally NOT mapped here - the
/// outbound B2C notification resolves market itself via docs/events/shared/country-code-lookup.md,
/// not from this inbound field (docs/events/inventory.StockOnHandUpdated.md §7.1). Mapped from the
/// Avro type by <see cref="Mappers.StockOnHandUpdatedEventMapper"/> (hand-written, no mapping library -
/// see that class's remarks).
/// </summary>
public sealed record StockOnHandUpdatedEvent(
    InventoryEventChannel Channel,
    string ReferenceId,
    DateTime UpdatedDate,
    InventoryEventLocation Location,
    string? Entity,
    string ProductId,
    string ProductUnits,
    string? Barcode,
    IReadOnlyList<StockOnHandQuantityDetail> QuantityDetails,
    StockOnHandUpdatedReason Reason);

/// <summary>One reported absolute quantity for a (Domain, State, Status, CountryOfOrigin, Hallmarking) combination.</summary>
/// <param name="CountryOfOrigin">Raw ISO 3166-1 alpha-2 code string, not a mirrored enum - same reasoning as <see cref="InventoryEventItemLine.CountryOfOrigin"/>.</param>
public sealed record StockOnHandQuantityDetail(
    int Quantity,
    InventoryEventStateSnapshot State,
    string CountryOfOrigin,
    string Hallmarking,
    StockOnHandInventoryDomain Domain);

/// <summary>Inventory domain assigned during inventory segmentation - which order type/area the stock is allocated to.</summary>
public enum StockOnHandInventoryDomain
{
    Unknown,
    B2B,
    B2C,
    InternalHallmarking,
    ExternalHallmarking,
    Omni,
}

/// <summary>Reason code reported for the absolute on-hand quantity snapshot (Avro <c>net.pandora.nexus.object.inventory.ReasonCode</c>).</summary>
public enum StockOnHandUpdatedReason
{
    Unknown,
    Adjustment,
    Bundling,
    Counting,
    CustomerReturn,
    Other,
    Receipt,
    ReceiptAdjustment,
    Return,
    Sale,
    Transfer,
    VendorReturn,
    AutoReconciliation,
}
