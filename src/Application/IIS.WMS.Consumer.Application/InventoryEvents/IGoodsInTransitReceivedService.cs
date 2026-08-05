using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Port for §3.2/§3.7 goods-in-transit receipt application
/// (docs/events/b2b.purchase.GoodsInTransitReceived.md) - kept as its own independent
/// service/retry-loop rather than folded into <see cref="IItemStockInventorySegmentationService"/> or
/// <see cref="IItemStockInventoryExtensionCalculationService"/>, mirroring why
/// <see cref="IOrderToInventoryAllocatedService"/> keeps its own loop: this event's create-or-increment
/// shape (sellable main record vs. non-sellable extended record) doesn't fit either existing service's
/// allocation/extension semantics.
/// </summary>
public interface IGoodsInTransitReceivedService
{
    /// <summary>
    /// Applies one shipment line's received quantity - §3.2 to the sellable
    /// <see cref="Domain.Aggregates.ItemStockInventory"/> record when <paramref name="isSellable"/>, or §3.7 to the
    /// non-sellable <see cref="Domain.Aggregates.ItemStockInventoryExtended"/> record keyed by
    /// (<paramref name="state"/>, <paramref name="status"/>) otherwise. Creates the target record with the
    /// received quantity seeded if it doesn't already exist; otherwise accumulates via a Cosmos Patch
    /// increment guarded by an ETag match, retried on a concurrency conflict.
    /// </summary>
    /// <param name="fulfilmentId">Destination fulfilment location id (§3.4).</param>
    /// <param name="itemCode">Item/product code from the shipment line.</param>
    /// <param name="countryOfOrigin">ISO country of origin from the shipment line.</param>
    /// <param name="hallmark">Hallmarking value from the shipment line.</param>
    /// <param name="quantity">Received quantity to accumulate (§3 - always non-negative for a receipt).</param>
    /// <param name="isSellable">§3.2 sellability gate - <see langword="true"/> routes to the main sellable record.</param>
    /// <param name="state">§3.3 resolved stock state - only meaningful for the non-sellable path.</param>
    /// <param name="status">§3.3 resolved stock status - only meaningful for the non-sellable path.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task<GoodsInTransitReceiptResult> ReceiveShipmentLineAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark, int quantity,
        bool isSellable, State state, Status status, CancellationToken cancellationToken = default);
}
