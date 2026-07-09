using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Azure.Cosmos;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Port for inventory aggregate persistence (cosmos-db.instructions.md §5). Controllers and other
/// Application services never reference <c>CosmosClient</c>/<c>Container</c> directly - only this
/// interface, implemented by <c>Infrastructure.Persistence.CosmosDb.InventoryEventRepository</c>.
/// Every <c>expectedETag</c> parameter below is required, not optional, on a mutating call - see
/// cosmos-db.instructions.md §9 for why last-write-wins is never acceptable for a
/// quantity-bearing entity.
/// </summary>
public interface IInventoryEventRepository
{
    /// <summary>Reads a single aggregate by id, or <see langword="null"/> if it doesn't exist.</summary>
    /// <param name="id">Aggregate id.</param>
    /// <param name="partitionKey">Cosmos partition key (<c>WarehouseId:Sku</c>).</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    Task<InventoryEvent?> GetAsync(string id, string partitionKey, CancellationToken cancellationToken = default);

    /// <summary>Creates a new item. A duplicate create for a deterministic id (redelivery) returns the existing item instead of throwing.</summary>
    /// <param name="entity">Aggregate to persist.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<InventoryEvent> CreateAsync(InventoryEvent entity, CancellationToken cancellationToken = default);

    /// <summary>Replaces an existing item, guarded by an ETag match. Throws <see cref="Domain.Exceptions.ConcurrencyException"/> on a mismatch.</summary>
    /// <param name="entity">Aggregate with its new state to persist.</param>
    /// <param name="expectedETag">ETag the stored item is expected to still have.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<InventoryEvent> ReplaceAsync(
        InventoryEvent entity, string expectedETag, CancellationToken cancellationToken = default);

    /// <summary>Applies a partial update via the Cosmos Patch API, guarded by an ETag match. At most 10 operations per call.</summary>
    /// <param name="id">Aggregate id.</param>
    /// <param name="partitionKey">Cosmos partition key (<c>WarehouseId:Sku</c>).</param>
    /// <param name="expectedETag">ETag the stored item is expected to still have.</param>
    /// <param name="operations">Patch operations to apply (Add/Set/Replace/Remove/Increment).</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<InventoryEvent> PatchAsync(
        string id, string partitionKey, string expectedETag,
        IReadOnlyList<PatchOperation> operations, CancellationToken cancellationToken = default);

    /// <summary>Deletes an item. Idempotent - deleting an item that no longer exists is not an error.</summary>
    /// <param name="id">Aggregate id.</param>
    /// <param name="partitionKey">Cosmos partition key (<c>WarehouseId:Sku</c>).</param>
    /// <param name="cancellationToken">Token to cancel the delete.</param>
    Task DeleteAsync(string id, string partitionKey, CancellationToken cancellationToken = default);

    /// <summary>Runs a filtered, paged query over full aggregates.</summary>
    /// <param name="options">Filter/sort/paging options. Throws if no partition key is supplied and cross-partition scanning isn't explicitly allowed.</param>
    /// <param name="cancellationToken">Token to cancel the query.</param>
    Task<PagedResult<InventoryEvent>> GetPagedAsync(
        QueryOptions<InventoryEvent> options, CancellationToken cancellationToken = default);

    /// <summary>Runs a filtered, paged, projected query - use when only a few fields are needed, to reduce RU cost and payload size.</summary>
    /// <typeparam name="TResult">Shape of the projected result.</typeparam>
    /// <param name="options">Filter/sort/paging/projection options. Throws if no partition key is supplied and cross-partition scanning isn't explicitly allowed.</param>
    /// <param name="cancellationToken">Token to cancel the query.</param>
    Task<PagedResult<TResult>> QueryAsync<TResult>(
        QueryOptions<InventoryEvent, TResult> options, CancellationToken cancellationToken = default);
}
