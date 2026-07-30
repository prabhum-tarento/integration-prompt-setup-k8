using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Handlers;

/// <summary>
/// Infrastructure-layer helper for low-level B2C extension and delta-to-OMS calculations.
/// Delegates to application services for the orchestration; this class encapsulates only the
/// formula computation and repository lookup side effects. Deprecated in favor of injecting
/// <see cref="IItemStockInventoryExtensionCalculationService"/> directly - kept for backward
/// compatibility with any handlers that may use it directly.
/// </summary>
public sealed class ItemStockInventoryCalculation
{
    private readonly IFulfilmentLevelSegmentationRepository _fulfilmentLevelSegmentationRepository;

    public ItemStockInventoryCalculation(
        IFulfilmentLevelSegmentationRepository fulfilmentLevelSegmentationRepository)
    {
        _fulfilmentLevelSegmentationRepository = fulfilmentLevelSegmentationRepository;
    }

    /// <summary>
    /// Recalculates B2C extension values and OMS delta when a pick/unpick event is processed on an
    /// extended-inventory record. Updates <c>B2CExtended</c> and <c>B2CAvailable</c> in the provided
    /// aggregate, and populates <paramref name="deltaResult"/> with the change metrics for downstream
    /// OMS relay. Mirrors Reflex's <c>CalculateB2CExtensionAsync</c>.
    /// </summary>
    /// <param name="prevB2CAvailable">The B2C available quantity before the pick/unpick was applied.</param>
    /// <param name="itemStockInventory">The aggregate to update in place.</param>
    /// <param name="deltaResult">Output object to populate with <c>IsB2CChanged</c> and delta amount.</param>
    /// <param name="cancellationToken">Token to cancel any async repository operations.</param>
    /// <returns>Whether an item-level segmentation rule matched (in Reflex, determines whether to call additional segmentation handlers).</returns>
    public async Task<bool> CalculateB2CExtensionAsync(
        int prevB2CAvailable,
        ItemStockInventory itemStockInventory,
        ItemStockInventoryDeltaResult deltaResult,
        CancellationToken cancellationToken = default)
    {
        var storeLeverage = await GetStoreLeverageAsync(itemStockInventory, cancellationToken);

        // Recalculate B2CExtended based on current B2B available after the mutation.
        itemStockInventory.CalculateB2CExtended();

        var newB2CAvailable = itemStockInventory.CalculateB2CAvailable();

        if (newB2CAvailable != prevB2CAvailable)
        {
            deltaResult.IsB2CChanged = true;
            deltaResult.DeltaTowardsOms = newB2CAvailable - prevB2CAvailable;
            itemStockInventory.UpdateB2CAvailable(newB2CAvailable);
        }

        return storeLeverage > 0;
    }

    private async Task<decimal> GetStoreLeverageAsync(
        ItemStockInventory itemStockInventory,
        CancellationToken cancellationToken)
    {
        var rule = await _fulfilmentLevelSegmentationRepository.GetFulfilmentLevelFulfilmentyByCategory(
            itemStockInventory.FulfilmentId,
            itemStockInventory.Hallmark,
            cancellationToken);

        return rule?.IsActive == true ? (rule.StoreLeveragePercentage ?? 0) : 0;
    }
}
