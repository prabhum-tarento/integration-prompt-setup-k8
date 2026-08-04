using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Port for §5.4 stock-sync discrepancy persistence (docs/events/inventory.StockSyncSubmitted.md),
/// implemented by <c>Infrastructure.Persistence.CosmosDb.Repository.ItemDiscrepencyDetailRepository</c>.
/// Only <see cref="UpsertAsync"/> is exposed - this data is an unordered write-once diagnostic record
/// with no read-modify-write step, so there is no ETag-guarded replace/patch surface to expose here.
/// </summary>
public interface IItemDiscrepencyDetailRepository
{
    /// <summary>
    /// Unconditionally overwrites the item at <paramref name="entity"/>'s partition key with its
    /// current state - last write wins, no ETag check. Correct here because a redelivered message
    /// saving the same discrepancy twice under the same deterministic id is expected, not concurrently
    /// contested state.
    /// </summary>
    /// <param name="entity">Record to persist.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<ItemDiscrepencyDetail> UpsertAsync(ItemDiscrepencyDetail entity, CancellationToken cancellationToken = default);
}
