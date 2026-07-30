using IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// §3.5 extended-inventory segmentation (docs/events/inventory.InventoryStateChanged.md) - ported from
/// the upstream Reflex facade's <c>ExtendedInventorySegmentationEventHandler</c>. Tracks quantity
/// sitting in a non-standard (State, Status) pair, distinct from the baseline
/// <see cref="Domain.Aggregates.ItemStockInventory"/> record. Runs unconditionally alongside §3.3,
/// whenever either side of the transition isn't the baseline Available/Pickable pair.
/// </summary>
public interface IItemStockInventoryExtendedSegmentationService
{
    /// <summary>
    /// Applies the to-state and from-state branches of extended-inventory segmentation for one
    /// fulfilment/item/hallmark/COO combination and state transition.
    /// </summary>
    /// <param name="fulfilmentId">Fulfilment location the transition occurred at.</param>
    /// <param name="itemCode">Item code being segmented.</param>
    /// <param name="hallmark">Hallmarking value of the item line.</param>
    /// <param name="countryOfOrigin">Country of origin of the item line.</param>
    /// <param name="fromState">The transition's origin state.</param>
    /// <param name="fromStatus">The transition's origin status.</param>
    /// <param name="toState">The transition's destination state.</param>
    /// <param name="toStatus">The transition's destination status.</param>
    /// <param name="quantity">Signed quantity moved by the transition.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task ApplyAsync(
        string fulfilmentId, string itemCode, string hallmark, string countryOfOrigin,
        State fromState, Status fromStatus, State toState, Status toStatus,
        int? quantity, CancellationToken cancellationToken = default);
}
