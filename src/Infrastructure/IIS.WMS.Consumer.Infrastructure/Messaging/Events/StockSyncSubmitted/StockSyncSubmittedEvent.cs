using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockSyncSubmitted;

/// <summary>
/// This consumer's own decoupled wire contract for a <c>StockSyncSubmitted</c> event - mirrors
/// <c>net.pandora.nexus.event.inventory.StockSyncSubmitted</c> (the Avro-generated SpecificRecord
/// from the NexusFacades.Common.AvroSchemas package) field-for-field, but as a plain type with no
/// Avro codegen ties, same rationale as <see cref="InventoryStateChangedEvent"/>. Reuses that
/// type's <see cref="InventoryEventChannel"/> and <see cref="InventoryEventLocation"/> since both
/// Avro events share those exact shapes. Mapped from the Avro type by
/// <see cref="Mappers.StockSyncSubmittedEventMapper"/> (hand-written, no mapping library - see
/// that class's remarks).
/// </summary>
public sealed record StockSyncSubmittedEvent(
    InventoryEventChannel Channel,
    DateTime SyncDate,
    InventoryEventLocation Location,
    string? Entity,
    string ProductId,
    string ProductUnits,
    IReadOnlyList<StockSyncQuantityDetail> QuantityDetails);

/// <summary>One reported quantity for a (Domain, State, Status, CountryOfOrigin, Hallmarking) combination.</summary>
/// <param name="CountryOfOrigin">Raw ISO 3166-1 alpha-2 code string, not a mirrored enum - same reasoning as <see cref="InventoryEventItemLine.CountryOfOrigin"/>.</param>
public sealed record StockSyncQuantityDetail(
    int Quantity,
    InventoryEventStateSnapshot State,
    StockSyncInventoryDomain Domain,
    string CountryOfOrigin,
    string Hallmarking);

/// <summary>Inventory domain assigned during inventory segmentation - which order type/area the stock is allocated to.</summary>
public enum StockSyncInventoryDomain
{
    Unknown,
    B2B,
    B2C,
    InternalHallmarking,
    ExternalHallmarking,
    Omni,
}
