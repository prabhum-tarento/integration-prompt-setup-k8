using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged;

/// <summary>
/// This consumer's own decoupled wire contract for an <c>InternalHallmarkingStatusChanged</c> event -
/// mirrors <c>net.pandora.nexus.event.inventory.InternalHallmarkingStatusChanged</c> (the
/// Avro-generated SpecificRecord from the NexusFacades.Common.AvroSchemas package) field-for-field,
/// but as a plain type with no Avro codegen ties, same rationale as
/// <see cref="InventoryStateChanged.InventoryStateChangedEvent"/>. <see cref="ItemLine"/> is singular,
/// not a collection - confirmed against both the Avro schema and the compiled Avro type's generated
/// shape (docs/events/inventory.InternalHallmarkingStatusChanged.md §1). Reuses
/// <see cref="InventoryEventChannel"/>/<see cref="InventoryEventLocation"/>/<see cref="InventoryEventLocationType"/>/
/// <see cref="InventoryEventChangeType"/>/<see cref="InventoryEventStateSnapshot"/>/<see cref="InventoryEventStockState"/>/
/// <see cref="InventoryEventStockStatus"/> from <c>InventoryStateChanged</c>'s wire contract, since the
/// underlying Avro shapes (<c>Channel</c>/<c>Location</c>/<c>InventoryChangeType</c>/<c>InventoryState</c>/<c>State</c>/<c>Status</c>)
/// are identical - not duplicated types. <see cref="Status"/> is this event's OWN top-level status
/// (STARTED/PICKED/CHANGED/FINISHED), a distinct Avro enum from <c>InventoryEventStockStatus</c>
/// (which maps <c>inventoryState.status</c>) - see <see cref="Mappers.InternalHallmarkingStatusChangedEventMapper"/>'s
/// remarks for the disambiguation this collision requires.
/// </summary>
public sealed record InternalHallmarkingStatusChangedEvent(
    InventoryEventChannel Channel,
    Status Status,
    string Id,
    DateTime ChangeDate,
    InventoryEventLocation Location,
    string? Entity,
    InventoryEventChangeType Type,
    InventoryEventStateSnapshot InventoryState,
    HallmarkingItemLine ItemLine);

/// <summary>This event's own top-level status (docs/events/inventory.InternalHallmarkingStatusChanged.md §1) - distinct from <see cref="InventoryEventStockStatus"/>.</summary>
public enum Status
{
    Unknown,
    Started,
    Picked,
    Changed,
    Finished,
}

/// <summary>The single item line carried by an <see cref="InternalHallmarkingStatusChangedEvent"/>.</summary>
/// <param name="CountryOfOrigin">Raw ISO 3166-1 alpha-2 code string, not a mirrored enum - same reasoning as <see cref="InventoryStateChanged.InventoryEventItemLine.CountryOfOrigin"/>.</param>
/// <param name="HallmarkingFrom">Source hallmark value - <c>NON</c> when this is a pure creation (docs §3.4).</param>
/// <param name="HallmarkingTo">Destination hallmark value.</param>
public sealed record HallmarkingItemLine(
    string LineNum,
    string ProductId,
    int Quantity,
    string CountryOfOrigin,
    string HallmarkingFrom,
    string HallmarkingTo,
    string? ReasonCode);
