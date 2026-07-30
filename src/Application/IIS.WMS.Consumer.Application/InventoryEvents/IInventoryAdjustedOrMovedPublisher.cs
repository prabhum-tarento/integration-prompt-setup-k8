using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// §3.6 B2B adjusted/moved event publisher port (docs/InventoryStateChangedFullQueueTrigger.md) - ported
/// from the upstream Reflex facade's <c>InventoryAdjustedOrMovedEventHandler</c>. Implemented in the
/// Infrastructure layer (<c>InventoryAdjustedOrMovedPublisher</c>) since the real implementation depends
/// on Service Bus queue configuration (<c>InventoryPublishOptions</c>) - this port only exposes
/// Domain/Application-shaped parameters, never that Infrastructure options type.
/// </summary>
public interface IInventoryAdjustedOrMovedPublisher
{
    /// <summary>
    /// Publishes one B2B adjusted/moved event for every item line of the originating
    /// <c>InventoryStateChangedEvent</c>. Applies SAE-2798 (skips the publish entirely when
    /// <paramref name="fromState"/> equals <paramref name="toState"/> and neither is
    /// <see cref="State.AVAILABLE"/>, unless the correlation context already declares this a
    /// B2B_INVENTORY_ADJUSTED redelivery) and SAE-3032 (forces the outbound status to
    /// <see cref="Status.UNKNOWN"/> for whichever side isn't <see cref="State.AVAILABLE"/> - the
    /// caller's own state is never mutated) fixups, normalizes negative line quantities via
    /// <see cref="Math.Abs(int)"/>, and falls back to a new <see cref="Guid"/> when
    /// <paramref name="referenceId"/> is blank.
    /// </summary>
    /// <param name="channel">The originating event's channel.</param>
    /// <param name="id">The originating event's id.</param>
    /// <param name="adjustmentDate">The originating event's change date.</param>
    /// <param name="locationId">Fulfilment location id.</param>
    /// <param name="locationType">Fulfilment location type.</param>
    /// <param name="entity">The originating event's entity, if any.</param>
    /// <param name="fromState">The transition's origin state.</param>
    /// <param name="fromStatus">The transition's origin status.</param>
    /// <param name="toState">The transition's destination state.</param>
    /// <param name="toStatus">The transition's destination status.</param>
    /// <param name="referenceId">The originating event's reference id, or blank to generate one.</param>
    /// <param name="lines">Every item line of the originating event.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task PublishAsync(
        string channel,
        string id,
        DateTime adjustmentDate,
        string locationId,
        string locationType,
        string? entity,
        State fromState,
        Status fromStatus,
        State toState,
        Status toStatus,
        string? referenceId,
        IReadOnlyList<InventoryAdjustedOrMovedLine> lines,
        CancellationToken cancellationToken = default);
}
