namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Rules;

/// <summary>
/// The two <see cref="InventoryStateChangedEvent"/> transitions this consumer treats as pick/unpick -
/// shared by <see cref="InventoryStateChangedConsumerHostedService"/> (OrderArchive categorization)
/// and <see cref="Handlers.InventoryStateChangedHandler"/> (OrderTracking relay), ported from
/// the upstream Reflex facade's <c>InventoryStateChangedQueueTrigger.isPickEvent</c>/<c>isUnpickEvent</c>.
/// </summary>
internal static class InventoryStateTransitionRules
{
    /// <summary>Available/Pickable to Available/Prepared - a pick.</summary>
    public static bool IsPickableToPrepared(InventoryStateChangedEvent value) =>
        value.FromState.State == InventoryEventStockState.Available && value.FromState.Status == InventoryEventStockStatus.Pickable
        && value.ToState.State == InventoryEventStockState.Available && value.ToState.Status == InventoryEventStockStatus.Prepared;

    /// <summary>
    /// Available/Prepared to Available/Held, or Available/Prepared to Available/Pickable - an
    /// unpick. Widened to match Reflex's actual production rule
    /// (<c>InventoryStateChangedOrchestrator.cs</c>: <c>Prepared→Held || Prepared→Pickable</c>) -
    /// this repo previously only recognized Prepared→Held.
    /// </summary>
    public static bool IsUnpickTransition(InventoryStateChangedEvent value) =>
        value.FromState.State == InventoryEventStockState.Available && value.FromState.Status == InventoryEventStockStatus.Prepared
        && value.ToState.State == InventoryEventStockState.Available
        && (value.ToState.Status == InventoryEventStockStatus.Held || value.ToState.Status == InventoryEventStockStatus.Pickable);

    /// <summary>
    /// §3.3 segmentation trigger (docs/events/inventory.InventoryStateChanged.md) - either side of the
    /// transition is Available/Pickable. Independent of <see cref="IsPickableToPrepared"/>/
    /// <see cref="IsUnpickTransition"/>: the orchestrator only runs this branch in the else-case
    /// (neither pick nor unpick), per the trigger's own sequencing.
    /// </summary>
    public static bool IsSegmentationTrigger(InventoryStateChangedEvent value) =>
        IsAvailablePickable(value.FromState) || IsAvailablePickable(value.ToState);

    /// <summary>Whether a state/status snapshot is the baseline Available/Pickable pair - drives both the §3.3 segmentation trigger and the §3.5 extended-segmentation trigger (its negation).</summary>
    public static bool IsAvailablePickable(InventoryEventStateSnapshot snapshot) =>
        snapshot.State == InventoryEventStockState.Available && snapshot.Status == InventoryEventStockStatus.Pickable;
}
