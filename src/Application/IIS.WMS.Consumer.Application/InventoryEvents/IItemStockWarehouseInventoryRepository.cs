using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Azure.Cosmos;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Port for <see cref="ItemStockWarehouseInventory"/> persistence (cosmos-db.instructions.md §5,
/// docs/events/b2b.sales.ConsolidatedOrderShipped.md §5.3). Implemented by
/// <c>Infrastructure.Persistence.CosmosDb.Repository.ItemStockWarehouseInventoryRepository</c>,
/// which routes to a per-fulfilment-code container by parsing <paramref name="category"/>'s first
/// <c>:</c>-delimited segment (per Q3) - mirroring <c>ItemStockInventoryRepository</c>, no separate
/// fulfilment-code parameter is threaded through these CRUD methods.
/// </summary>
public interface IItemStockWarehouseInventoryRepository
{
    /// <summary>Reads a single record by id, or <see langword="null"/> if it doesn't exist.</summary>
    /// <param name="id">Record id.</param>
    /// <param name="category">Cosmos partition key (same value as <paramref name="id"/> - see <see cref="ItemStockWarehouseInventory.Category"/>).</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    Task<ItemStockWarehouseInventory?> GetAsync(string id, string category, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new record - the §3.3 step 3 create-if-missing path. Redelivery-safe: a Cosmos conflict
    /// on an already-created id is resolved by re-reading and returning the existing item rather than throwing.
    /// </summary>
    /// <param name="entity">Record to create.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<ItemStockWarehouseInventory> CreateAsync(ItemStockWarehouseInventory entity, CancellationToken cancellationToken = default);

    /// <summary>Applies a partial update via the Cosmos Patch API, guarded by an ETag match.</summary>
    /// <param name="id">Record id.</param>
    /// <param name="category">Cosmos partition key (same value as <paramref name="id"/>).</param>
    /// <param name="expectedETag">ETag the stored item is expected to still have.</param>
    /// <param name="operations">Patch operations to apply.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<ItemStockWarehouseInventory> PatchAsync(
        string id, string category, string expectedETag,
        IReadOnlyList<PatchOperation> operations, CancellationToken cancellationToken = default);
}
