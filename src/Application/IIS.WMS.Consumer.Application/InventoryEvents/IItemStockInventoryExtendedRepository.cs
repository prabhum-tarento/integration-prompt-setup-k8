using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Port for <see cref="ItemStockInventoryExtended"/> persistence (cosmos-db.instructions.md §5) - the
/// §3.5 extended-state inventory snapshot keyed by (FulfilmentId, ItemCode, Hallmark, COO, State, Status).
/// Controllers and other Application services never reference <c>CosmosClient</c>/<c>Container</c>
/// directly - only this interface, implemented by
/// <c>Infrastructure.Persistence.CosmosDb.Repository.ItemStockInventoryExtendedRepository</c>.
/// </summary>
public interface IItemStockInventoryExtendedRepository
{
    /// <summary>Reads a single record by its composite key, or <see langword="null"/> if it doesn't exist.</summary>
    Task<ItemStockInventoryExtended?> GetAsync(
        string fulfilmentId, string itemCode, string hallmark, string countryOfOrigin,
        State state, Status status, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new extended-state record - used by the §3.5 to-state create-if-missing path
    /// (docs/events/inventory.InventoryStateChanged.md). Redelivery-safe: a Cosmos conflict on an
    /// already-created id is resolved by re-reading and returning the existing item rather than throwing.
    /// </summary>
    Task<ItemStockInventoryExtended> CreateAsync(
        ItemStockInventoryExtended entity, CancellationToken cancellationToken = default);

    /// <summary>Replaces an existing item, guarded by an ETag match. Throws <see cref="IIS.WMS.Common.Exceptions.ConcurrencyException"/> on a mismatch.</summary>
    Task<ItemStockInventoryExtended> ReplaceAsync(
        ItemStockInventoryExtended entity, string expectedETag, CancellationToken cancellationToken = default);
}
