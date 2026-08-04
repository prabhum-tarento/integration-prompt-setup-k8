using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Application.InternalHallmarkingStatusChanged;

/// <summary>
/// Use-case orchestration for the internal-hallmarking STARTED/PICKED/CHANGED/FINISHED status paths
/// (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.1-§3.5) - the interface
/// <c>InternalHallmarkingStatusChangedHandler</c> depends on, mirroring
/// <see cref="InventoryEvents.IItemStockInventoryService"/>'s shape: it never touches the repository or
/// Cosmos types directly, and every method also runs the §3.5 <c>manageIntransitAsync</c> in-transit
/// bookkeeping for its status as "normal handler logic", per the doc's own note that there is no
/// separate orchestration step for it.
/// </summary>
public interface IInternalHallmarkingStatusChangedService
{
    /// <summary>
    /// §3.1 STARTED - allocates <paramref name="quantity"/> onto <c>B2BAllocated</c> for the given
    /// category, recalculating the B2C extension if active, then records an <c>ALLOCATED</c> transit
    /// leg. If the underlying <c>ItemStockInventory</c> record doesn't exist, or the resulting quantity
    /// would be invalid, logs the application-level rejection and returns a no-change result rather than
    /// throwing (docs §3.1/§8: <c>MISSING_INVENTORY</c>/<c>INVALID_QUANTITY</c> are business rejections,
    /// not poison messages).
    /// </summary>
    Task<ItemStockInventoryDeltaResult> AllocateAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default);

    /// <summary>
    /// §3.2/§3.3 PICKED - applies a B2B pick (reusing
    /// <see cref="InventoryEvents.IItemStockInventoryExtensionService.ApplyPickB2BWithExtensionAsync"/>,
    /// already B2C-extension-aware) followed by the consolidated-shipment step
    /// (<see cref="Domain.Aggregates.ItemStockInventory.ApplyConsolidatedShipment"/>), then records
    /// <c>PICKED</c>/<c>ALLOCATED</c> transit-leg movements. The wire event carries a single quantity
    /// and no confirmation-type signal, so <paramref name="quantity"/> is used for both the pick and the
    /// shipment step, defaulted to <see cref="ConfirmationType.STANDARD"/> (the §3.3 "direct shipment"
    /// row) - <see cref="ConfirmationType"/>'s PRELIMINARY/STANDARD_FOLLOWING_PRELIMINARY values are
    /// ported from the unrelated <c>b2b.sales.ConsolidatedOrderShipped</c> event and have no signal on
    /// this event's schema to select them.
    /// </summary>
    Task<ItemStockInventoryDeltaResult> PickAndShipAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default);

    /// <summary>
    /// §3.4 CHANGED - moves <paramref name="quantity"/> from <paramref name="hallmarkFrom"/> to
    /// <paramref name="hallmarkTo"/> via the existing
    /// <see cref="InventoryEvents.IItemStockInventorySegmentationService"/> (the same §3.3 segmentation
    /// logic, applied once per hallmark leg with an opposite-signed inbound quantity), then records
    /// <c>INTRANSIT</c> transit-leg movements for both hallmark legs.
    /// </summary>
    Task<ItemStockInventoryDeltaResult> ChangeHallmarkAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmarkFrom, string hallmarkTo,
        int quantity, bool isThirdPartyLogistics, CancellationToken cancellationToken = default);

    /// <summary>
    /// §3.5 FINISHED - completes the transit by moving <paramref name="quantity"/> out of
    /// <c>InTransit</c> and into <c>B2BAvailable</c> on the <paramref name="hallmark"/> (target)
    /// category (<see cref="Domain.Aggregates.ItemStockInventory.CompleteInternalHallmarkingTransit"/>),
    /// then records the <c>INTRANSIT → CREATED</c> transit-leg movement.
    /// </summary>
    Task CompleteTransitAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default);
}
