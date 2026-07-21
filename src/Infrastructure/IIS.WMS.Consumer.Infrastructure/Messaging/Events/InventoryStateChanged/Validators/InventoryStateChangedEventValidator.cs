using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Validators;

/// <summary>
/// Business-rule validation for one deserialized <see cref="InventoryStateChangedEvent"/>, run by
/// <see cref="InventoryStateChangedConsumerHostedService"/>'s <c>ValidateAsync</c> override - ported
/// from the upstream Reflex facade's own <c>InventoryStateChangedKafkaHandler.CanProcessMessage</c>
/// (<c>IIS.WMS.Reflex.Application</c>), adapted to this consumer's own <see cref="InventoryStateChangedEvent"/>
/// wire contract and enums. Only the pass/fail decision is ported - the Reflex handler's <c>MoveSign</c>
/// side effect has no equivalent here, since nothing downstream of this relay consumes it. A message
/// failing these rules is valid but deliberately not relayed (returns <see langword="false"/>, not a
/// throw) - see <see cref="KafkaConsumerHostedServiceBase.CreateSchemaHandler{TValue}"/>'s
/// <c>validateAsync</c> remarks for the throw-vs-return-false distinction.
/// </summary>
internal static class InventoryStateChangedEventValidator
{
    /// <summary>
    /// Mirrors <c>CanProcessMessage</c>: rejects a no-op transition (identical from/to state and
    /// status), a transition into/out of <see cref="InventoryEventStockStatus.Hallmarking"/>, and -
    /// unless the location is <see cref="FulfilmentLocationIds.Tdc"/>/<see cref="FulfilmentLocationIds.Adc"/> -
    /// a transition between two <see cref="InventoryEventStockStatus.Held"/> states. At
    /// <see cref="FulfilmentLocationIds.Edc"/>, additionally requires either side of the transition to
    /// be <see cref="InventoryEventStockState.Available"/>.
    /// </summary>
    public static bool CanProcess(InventoryStateChangedEvent inventoryStateChanged) =>
        GetRejectionReason(inventoryStateChanged) is null;

    /// <summary>
    /// Same rule set as <see cref="CanProcess"/>, but returns the specific reason a transition was
    /// rejected instead of a bare <see langword="false"/> - lets the caller log which rule tripped
    /// rather than just that validation failed. Returns <see langword="null"/> when the transition is
    /// valid.
    /// </summary>
    public static string? GetRejectionReason(InventoryStateChangedEvent inventoryStateChanged)
    {
        var isValidLocationState =
            inventoryStateChanged.Location.Id != FulfilmentLocationIds.Edc
            || inventoryStateChanged.FromState.State == InventoryEventStockState.Available
            || inventoryStateChanged.ToState.State == InventoryEventStockState.Available;

        if (!isValidLocationState)
        {
            return $"Location {inventoryStateChanged.Location.Id} requires either side of the transition to be " +
                $"{InventoryEventStockState.Available} (From={inventoryStateChanged.FromState.State}, " +
                $"To={inventoryStateChanged.ToState.State}).";
        }

        if (inventoryStateChanged.FromState.State == inventoryStateChanged.ToState.State
            && inventoryStateChanged.FromState.Status == inventoryStateChanged.ToState.Status)
        {
            return $"No-op transition: From and To state/status are both " +
                $"{inventoryStateChanged.FromState.State}/{inventoryStateChanged.FromState.Status}.";
        }

        var bothHeld = inventoryStateChanged.FromState.Status == InventoryEventStockStatus.Held
            && inventoryStateChanged.ToState.Status == InventoryEventStockStatus.Held;

        if (bothHeld)
        {
            return inventoryStateChanged.Location.Id == FulfilmentLocationIds.Tdc
                || inventoryStateChanged.Location.Id == FulfilmentLocationIds.Adc
                ? null
                : $"Held-to-Held transition is only allowed at {FulfilmentLocationIds.Tdc}/{FulfilmentLocationIds.Adc}, " +
                    $"not {inventoryStateChanged.Location.Id}.";
        }

        if (inventoryStateChanged.FromState.Status == InventoryEventStockStatus.Hallmarking
            || inventoryStateChanged.ToState.Status == InventoryEventStockStatus.Hallmarking)
        {
            return $"Transition involves {InventoryEventStockStatus.Hallmarking} status " +
                $"(From={inventoryStateChanged.FromState.Status}, To={inventoryStateChanged.ToState.Status}).";
        }

        return null;
    }
}
