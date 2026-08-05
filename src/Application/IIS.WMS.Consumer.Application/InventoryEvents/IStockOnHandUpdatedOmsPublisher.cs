using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// §7.3/§9 B2C stock notification publisher port (docs/events/inventory.StockOnHandUpdated.md).
/// Unlike <see cref="IStockSyncSubmittedOmsPublisher"/>'s hardcoded BR/CA market rule, market
/// resolution here follows <see cref="IDeltaTowardsOmsPublisher"/>'s pattern - a
/// <see cref="IFulfilmentUnitRepository"/>/<see cref="ICountryRepository"/> lookup with a fail-safe
/// <c>"UNKNOWN"</c> fallback (shared/country-code-lookup.md) - since this doc's own §7.1 field table
/// carries no inbound Market field to derive a shortcut from. Publishing is unconditional per group
/// (no feature-flag/availability gate), unlike <see cref="IStockSyncSubmittedOmsPublisher"/>'s gated
/// publish - see §7.3/§9.
/// </summary>
public interface IStockOnHandUpdatedOmsPublisher
{
    /// <summary>
    /// Publishes one B2C stock notification for a single (CountryOfOrigin, Hallmarking) group.
    /// </summary>
    /// <param name="fulfilmentId">Resolved internal fulfilment id (post BRZ3PL mapping).</param>
    /// <param name="locationType">Fulfilment location type, as reported on the inbound event.</param>
    /// <param name="productId">Item/product code.</param>
    /// <param name="productUnits">Product units, as reported on the inbound event.</param>
    /// <param name="entity">Entity, as reported on the inbound event, or <see langword="null"/> if absent.</param>
    /// <param name="barcode">Barcode, as reported on the inbound event, or <see langword="null"/> if absent.</param>
    /// <param name="quantityDetails">The group's relevant (filtered) quantity lines.</param>
    /// <param name="reason">Reason code, as reported on the inbound event.</param>
    /// <param name="updatedDate">Updated date, as reported on the inbound event.</param>
    /// <param name="eventId">Originating event/group id, used to build a deterministic message id so a
    /// redelivered publish is de-duplicated downstream - never a fresh <see cref="Guid"/>.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task PublishAsync(
        string fulfilmentId,
        string locationType,
        string productId,
        string productUnits,
        string? entity,
        string? barcode,
        IReadOnlyList<StockOnHandUpdatedOmsQuantityDetail> quantityDetails,
        string reason,
        DateTime updatedDate,
        string eventId,
        CancellationToken cancellationToken = default);
}
