namespace IIS.WMS.Consumer.Infrastructure.Messaging;

/// <summary>
/// This consumer's own decoupled wire contract for an <c>InventoryAdjusted</c> event - mirrors
/// <c>net.pandora.nexus.event.inventory.InventoryAdjusted</c> (the Avro-generated SpecificRecord from
/// the NexusFacades.Common.AvroSchemas package) field-for-field, but as a plain type with no Avro
/// codegen ties, same rationale as <see cref="InventoryStateChangedEvent"/>. Reuses that type's
/// <see cref="InventoryEventChannel"/>, <see cref="InventoryEventLocation"/>,
/// <see cref="InventoryEventChangeType"/>, and <see cref="InventoryEventItemLine"/> (and its own
/// nested weight/money types) since both Avro events share those exact shapes. Mapped from the Avro
/// type by <see cref="Kafka.InventoryAdjustedEventMapper"/> (hand-written, no mapping library - see
/// that class's remarks).
/// </summary>
public sealed record InventoryAdjustedEvent(
    InventoryEventChannel Channel,
    InventoryEventAdjustment Adjustment);

/// <summary>One inventory adjustment, covering manual/automatic adjustments and physical-count changes at a location.</summary>
public sealed record InventoryEventAdjustment(
    string ReferenceId,
    DateTime AdjustmentDate,
    string? Entity,
    InventoryEventChangeType Type,
    InventoryEventStateSnapshot State,
    InventoryEventLocation Location,
    InventoryEventReasonCode Reason,
    IReadOnlyList<InventoryEventItemLine> AdjustmentLines);

/// <summary>Why the inventory adjustment happened.</summary>
public enum InventoryEventReasonCode
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
