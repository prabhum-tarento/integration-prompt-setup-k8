using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// §3.3 segmentation/extension orchestration (docs/events/inventory.InventoryStateChanged.md) - ported
/// from the upstream Reflex facade's <c>InventorySegmentationAndExtensionHandler</c>/
/// <c>updateItemLevelSegmentationHandlerAsync</c>. Unlike <see cref="IItemStockInventoryService"/>, a
/// missing <see cref="Domain.Aggregates.ItemStockInventory"/> record is not a reject - one is created
/// with zero-initialized quantities via <see cref="Domain.Aggregates.ItemStockInventory.CreateDefault"/>.
/// </summary>
public interface IItemStockInventorySegmentationService
{
    /// <summary>
    /// Applies inbound segmentation for one fulfilment/item/hallmark/COO combination, creating the
    /// record if missing. If the record was created and <paramref name="inboundQty"/> is negative
    /// (cannot negate empty inventory), logs a warning and returns without mutating. Then, unless
    /// <paramref name="fulfilmentId"/> is the TDC location, writes the resulting state back onto the
    /// matching item-level segmentation rule, if one exists.
    /// </summary>
    /// <param name="fulfilmentId">Fulfilment location the segmentation event occurred at.</param>
    /// <param name="itemCode">Item code being segmented.</param>
    /// <param name="countryOfOrigin">Country of origin of the item line.</param>
    /// <param name="hallmark">Hallmarking value of the item line.</param>
    /// <param name="inboundQty">Signed inbound quantity (positive = additive, negative = subtractive).</param>
    /// <param name="isThirdPartyLogistics">Whether the fulfilment location is a 3PL (drives fulfilment-level B2C-only segmentation vs. item/fulfilment-level segmentation).</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>OMS delta metrics (zero values if no delta occurred, or the negative-inbound-on-missing-record guard was hit).</returns>
    Task<ItemStockInventoryDeltaResult> ApplySegmentationAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int inboundQty, bool isThirdPartyLogistics, CancellationToken cancellationToken = default);
}
