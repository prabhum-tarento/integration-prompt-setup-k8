using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Azure.Cosmos;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Port for <see cref="ItemStockIntransit"/> persistence (cosmos-db.instructions.md §5), mirroring
/// <see cref="IItemStockInventoryRepository"/>'s shape for the §5.2 transit-tracking aggregate.
/// Controllers and other Application services never reference <c>CosmosClient</c>/<c>Container</c>
/// directly - only this interface, implemented by
/// <c>Infrastructure.Persistence.CosmosDb.Repository.ItemStockIntransitRepository</c>.
/// </summary>
public interface IItemStockIntransitRepository
{
    /// <summary>Reads a single record by id, or <see langword="null"/> if it doesn't exist.</summary>
    /// <param name="id">Record id.</param>
    /// <param name="category">Cosmos partition key (same value as <paramref name="id"/> - see <see cref="ItemStockIntransit.Category"/>).</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    Task<ItemStockIntransit?> GetAsync(string id, string category, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new item - used by the §5.2/§6 create-if-missing path for a status leg that has never
    /// been written to. Redelivery-safe: a Cosmos conflict on an already-created id is resolved by
    /// re-reading and returning the existing item rather than throwing.
    /// </summary>
    /// <param name="entity">Record to create.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<ItemStockIntransit> CreateAsync(ItemStockIntransit entity, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a partial update via the Cosmos Patch API, guarded by an ETag match - use this instead of
    /// a full replace whenever only a known subset of fields changed, since this container is shared
    /// with other applications and a full-document replace would silently overwrite fields they
    /// concurrently wrote. At most 10 operations per call.
    /// </summary>
    /// <param name="id">Record id.</param>
    /// <param name="category">Cosmos partition key (same value as <paramref name="id"/> - see <see cref="ItemStockIntransit.Category"/>).</param>
    /// <param name="expectedETag">ETag the stored item is expected to still have.</param>
    /// <param name="operations">Patch operations to apply (Add/Set/Replace/Remove/Increment).</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<ItemStockIntransit> PatchAsync(
        string id, string category, string expectedETag,
        IReadOnlyList<PatchOperation> operations, CancellationToken cancellationToken = default);
}
