using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Port for <see cref="Item"/> master-data existence lookups (assumption 3, docs/events/inventory.StockSyncSubmitted.md) -
/// items may be missing from IIS master data; the caller (not this repository) auto-creates a missing
/// item via <see cref="CreateAsync"/>, mirroring <c>ItemStockInventorySegmentationService</c>'s own
/// create-if-missing pattern rather than baking that decision into the repository.
/// </summary>
public interface IItemRepository
{
    /// <summary>Reads the item master record for an item code, or <see langword="null"/> if it doesn't exist.</summary>
    /// <param name="itemCode">Item code.</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    Task<Item?> GetByItemCodeAsync(string itemCode, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new item master record. A duplicate create for a deterministic id (redelivery)
    /// returns the existing item instead of throwing.
    /// </summary>
    Task<Item> CreateAsync(Item entity, CancellationToken cancellationToken = default);
}
