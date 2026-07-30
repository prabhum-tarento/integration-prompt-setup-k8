namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// §3.7 OMS delta synchronization publisher port (docs/events/shared/delta-towards-oms.md) - ported
/// from the upstream Reflex facade's <c>manageInternalHallmarkingAllocatedEventHandlerAsync</c>/
/// <c>getCountryCodeEventHandlerAsync</c> pair. Implemented in the Infrastructure layer
/// (<c>DeltaTowardsOmsPublisher</c>) since it depends on Service Bus queue configuration
/// (<c>InventoryPublishOptions</c>) - this port only exposes Domain/Application-shaped parameters.
/// </summary>
public interface IDeltaTowardsOmsPublisher
{
    /// <summary>
    /// Resolves the fulfilment location's country code via <see cref="IFulfilmentUnitRepository"/> (falling
    /// back to <c>"UNKNOWN"</c> if the fulfilment unit isn't found) and publishes one OMS delta event for
    /// the given item line's B2C availability change.
    /// </summary>
    /// <param name="productId">The item line's product/item code.</param>
    /// <param name="locationId">Fulfilment location id.</param>
    /// <param name="locationType">Fulfilment location type.</param>
    /// <param name="countryOfOrigin">The item line's country of origin.</param>
    /// <param name="hallmarking">The item line's hallmarking value.</param>
    /// <param name="deltaTowardsOms">The signed B2C-available delta to report.</param>
    /// <param name="eventId">The originating <c>InventoryStateChangedEvent.Id</c>, used to build a
    /// deterministic <c>ReferenceId</c> (<c>locationId:productId:eventId</c>, per
    /// docs/events/shared/delta-towards-oms.md) so a redelivered publish is de-duplicated downstream -
    /// never a fresh <see cref="Guid"/>.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task PublishAsync(
        string productId,
        string locationId,
        string locationType,
        string countryOfOrigin,
        string hallmarking,
        int deltaTowardsOms,
        string eventId,
        CancellationToken cancellationToken = default);
}
