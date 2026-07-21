using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Port for <see cref="ItemStockInventory"/> persistence (cosmos-db.instructions.md §5). Controllers
/// and other Application services never reference <c>CosmosClient</c>/<c>Container</c> directly -
/// only this interface, implemented by
/// <c>Infrastructure.Persistence.CosmosDb.Repository.ItemStockInventoryRepository</c>. Only the two
/// operations the pick/unpick retry loop needs are exposed - no paging/query methods (YAGNI; nothing
/// needs them yet).
/// </summary>
public interface IItemStockInventoryRepository
{
    /// <summary>Reads a single record by id, or <see langword="null"/> if it doesn't exist.</summary>
    /// <param name="id">Record id.</param>
    /// <param name="category">Cosmos partition key (same value as <paramref name="id"/> - see <see cref="ItemStockInventory.Category"/>).</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    Task<ItemStockInventory?> GetAsync(string id, string category, CancellationToken cancellationToken = default);

    /// <summary>Replaces an existing item, guarded by an ETag match. Throws <see cref="IIS.WMS.Common.Exceptions.ConcurrencyException"/> on a mismatch.</summary>
    /// <param name="entity">Record with its new state to persist.</param>
    /// <param name="expectedETag">ETag the stored item is expected to still have.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<ItemStockInventory> ReplaceAsync(
        ItemStockInventory entity, string expectedETag, CancellationToken cancellationToken = default);
}
