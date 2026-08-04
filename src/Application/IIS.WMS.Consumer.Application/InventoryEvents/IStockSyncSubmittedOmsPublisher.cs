namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// §3.5 OMS B2C stock snapshot publisher port (docs/events/inventory.StockSyncSubmitted.md). Unlike
/// <see cref="IDeltaTowardsOmsPublisher"/>, market resolution here is a hardcoded BR/CA rule keyed off
/// the resolved fulfilment id (the doc's own §3.5 "Market" rule), not a
/// FulfilmentUnitRepository/CountryRepository lookup - this event only ever reports for those two
/// markets. Feature-flag and availability gating (§3.5 "Feature gate"/"Availability gate") are the
/// caller's responsibility, mirroring how <c>InventoryAdjustedHandler</c> gates
/// <see cref="IDeltaTowardsOmsPublisher"/>/<see cref="IInventoryComparisonReportPublisher"/> itself
/// rather than pushing gating into the publisher.
/// </summary>
public interface IStockSyncSubmittedOmsPublisher
{
    /// <summary>
    /// Publishes one B2C stock snapshot for the given item. <paramref name="fulfilmentId"/> must already
    /// be resolved (BRZ3PLConsigneeId → BRZDC3PLFulfilmentId, per §3.1) - this method reverse-maps it back
    /// to BRZ3PLConsigneeId in the outgoing report's <c>Location.Id</c> (§3.5 "Location round-trip") and
    /// derives <c>Market</c> from it (BRZDC3PLFulfilmentId → BR, else CA).
    /// </summary>
    /// <param name="fulfilmentId">Resolved internal fulfilment id (post BRZ3PL mapping).</param>
    /// <param name="locationType">Fulfilment location type, as reported on the inbound event.</param>
    /// <param name="itemCode">Item/product code.</param>
    /// <param name="b2cAvailableQuantity">Current B2CAVL quantity to report as AVAILABLE/PICKABLE.</param>
    /// <param name="eventId">Originating event id, used to build a deterministic message id so a
    /// redelivered publish is de-duplicated downstream - never a fresh <see cref="Guid"/>.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task PublishAsync(
        string fulfilmentId,
        string locationType,
        string itemCode,
        int b2cAvailableQuantity,
        string eventId,
        CancellationToken cancellationToken = default);
}
