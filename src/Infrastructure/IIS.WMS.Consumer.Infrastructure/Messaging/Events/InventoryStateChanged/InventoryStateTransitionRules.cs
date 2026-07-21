namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

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
}
