using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.BulkInventoryImport;

/// <summary>
/// Port for bulk-import item persistence (cosmos-db.instructions.md §5), implemented by
/// <c>Infrastructure.Persistence.CosmosDb.InventoryBulkImportItemRepository</c>. Only
/// <see cref="UpsertAsync"/> is exposed - unlike <c>IInventoryEventRepository</c>, this data is an
/// unordered, idempotent snapshot reload with no read-modify-write step, so there is no ETag-guarded
/// replace/patch surface to expose here.
/// </summary>
public interface IBulkInventoryImportRepository
{
    /// <summary>
    /// Unconditionally overwrites the item at <paramref name="entity"/>'s partition key with its
    /// current state - last write wins, no ETag check. Correct here because bulk-import data is not
    /// concurrently contested the way <c>InventoryEvent</c>'s reserve/allocate balance is.
    /// </summary>
    /// <param name="entity">Item to persist.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<InventoryBulkImportItem> UpsertAsync(InventoryBulkImportItem entity, CancellationToken cancellationToken = default);
}
