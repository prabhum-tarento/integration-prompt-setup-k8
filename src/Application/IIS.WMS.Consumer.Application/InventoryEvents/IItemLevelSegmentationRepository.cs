using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

public interface IItemLevelSegmentationRepository
{
    Task<ItemLevelSegmentation?> GetItemLevelFulfilmentyByCategory(string fulfilment, string hallMarkType, string itemCode, string coo);

    /// <summary>
    /// Writes back the rule's "updated through inventory" fields (<c>CurrentOmniStock</c>/<c>CurrentEcomStock</c>/
    /// <c>StoreShare</c>/<c>EcomStatus</c>/<c>IsExtended</c>/<c>LastModified</c>) after a §3.3 item-level
    /// segmentation/extension pass (docs/events/inventory.InventoryStateChanged.md). ETag-guarded, matching
    /// every other mutation in this repo (cosmos-db.instructions.md §6); throws
    /// <see cref="IIS.WMS.Common.Exceptions.ConcurrencyException"/> on a mismatch.
    /// </summary>
    /// <param name="entity">Rule with its new state to persist - must carry the ETag last read from Cosmos.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<ItemLevelSegmentation> UpdateItemLevelFulfilmentAsync(ItemLevelSegmentation entity, CancellationToken cancellationToken = default);
}