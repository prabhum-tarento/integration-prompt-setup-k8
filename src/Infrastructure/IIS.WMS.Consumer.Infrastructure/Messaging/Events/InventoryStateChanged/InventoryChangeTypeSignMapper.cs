namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

/// <summary>
/// Maps an <see cref="InventoryEventChangeType"/> to the signed direction §3.3/§3.5 apply an item
/// line's quantity with (docs/InventoryStateChangedFullQueueTrigger.md §4 Formula 1). This repo's
/// wire contract carries no <c>MoveSign</c> string field (Reflex's own <c>MoveSign + Quantity</c>
/// string-concatenation trick is Reflex-only) - the sign is instead derived from the change type
/// itself, per the mapping agreed for this port.
/// </summary>
internal static class InventoryChangeTypeSignMapper
{
    private static readonly HashSet<InventoryEventChangeType> AdditiveTypes =
    [
        InventoryEventChangeType.Cin,
        InventoryEventChangeType.Ent,
        InventoryEventChangeType.Mav,
        InventoryEventChangeType.Rfr,
        InventoryEventChangeType.Rrt,
    ];

    private static readonly HashSet<InventoryEventChangeType> SubtractiveTypes =
    [
        InventoryEventChangeType.Blc,
        InventoryEventChangeType.Cie,
        InventoryEventChangeType.Cmd,
        InventoryEventChangeType.Min,
        InventoryEventChangeType.Mrp,
        InventoryEventChangeType.Mpr,
        InventoryEventChangeType.Mqa,
        InventoryEventChangeType.Mqp,
        InventoryEventChangeType.Oia,
        InventoryEventChangeType.Rtr,
    ];

    /// <summary>
    /// Resolves the signed inbound quantity for one item line. An unrecognized type (e.g.
    /// <see cref="InventoryEventChangeType.Unknown"/>) defaults to additive, matching Reflex's own
    /// "MoveSign defaults to empty string (no sign = addition)" null-handling rule.
    /// </summary>
    public static int GetSignedQuantity(InventoryEventChangeType type, int quantity) =>
        SubtractiveTypes.Contains(type) ? -quantity : quantity;
}
