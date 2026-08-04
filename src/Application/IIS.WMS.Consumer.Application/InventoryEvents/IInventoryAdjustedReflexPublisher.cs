using IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// FINISHED-status inventory-adjusted publisher port
/// (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.5/§9) - a narrow, single-item-line
/// port distinct from <see cref="IInventoryAdjustedOrMovedPublisher"/>, since that port targets the
/// unrelated §3.6 SAP <c>nexus-producer</c> path (its own SAE-2798/SAE-3032 fixups included) while this
/// one targets the <c>inventory-adjusted-reflex</c> queue documented for internal hallmarking's FINISHED
/// transit-completion step. Implemented in the Infrastructure layer
/// (<c>InventoryAdjustedReflexPublisher</c>) since it depends on Service Bus queue configuration
/// (<c>InventoryPublishOptions</c>) - this port only exposes Domain/Application-shaped parameters.
/// </summary>
public interface IInventoryAdjustedReflexPublisher
{
    /// <summary>
    /// Publishes one inventory-adjusted event for the completed transit's single item line.
    /// </summary>
    /// <param name="channel">The originating event's channel.</param>
    /// <param name="id">The originating event's id.</param>
    /// <param name="adjustmentDate">The originating event's change date.</param>
    /// <param name="locationId">Fulfilment location id (the <c>HallmarkTo</c> target location).</param>
    /// <param name="locationType">Fulfilment location type.</param>
    /// <param name="entity">The originating event's entity, if any.</param>
    /// <param name="itemCode">The item line's product id.</param>
    /// <param name="quantity">Quantity moved from <c>InTransit</c> into <c>B2BAvailable</c>.</param>
    /// <param name="countryOfOrigin">The item line's country of origin.</param>
    /// <param name="hallmarkTo">The item line's destination hallmark value.</param>
    /// <param name="toState">The completed transition's destination state.</param>
    /// <param name="toStatus">The completed transition's destination status.</param>
    /// <param name="referenceId">The originating event's id, used to build a deterministic downstream reference - never a fresh <see cref="Guid"/>.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task PublishAsync(
        string channel,
        string id,
        DateTime adjustmentDate,
        string locationId,
        string locationType,
        string? entity,
        string itemCode,
        int quantity,
        string countryOfOrigin,
        string hallmarkTo,
        State toState,
        Status toStatus,
        string referenceId,
        CancellationToken cancellationToken = default);
}
