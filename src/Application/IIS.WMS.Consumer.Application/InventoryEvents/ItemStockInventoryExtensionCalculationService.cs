using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Application-layer implementation of B2C extension and OMS delta calculation - delegates to the
/// infrastructure's formula helpers via <see cref="IFulfilmentLevelSegmentationRepository"/>.
/// </summary>
public sealed class ItemStockInventoryExtensionCalculationService(
    IItemLevelSegmentationRepository itemLevelSegmentationRepository,
    IFulfilmentLevelSegmentationRepository fulfilmentLevelSegmentationRepository,
    ILogger<ItemStockInventoryExtensionCalculationService> logger) : IItemStockInventoryExtensionCalculationService
{
    /// <inheritdoc />
    public async Task CalculateB2CExtensionAsync(
        int prevB2CAvailable,
        ItemStockInventory itemStockInventory,
        ItemStockInventoryDeltaResult deltaResult,
        CancellationToken cancellationToken = default)
    {
        // Store leverage only gates whether an item-level rule exists for this combination - the
        // recalculated formula below (ported verbatim from Reflex's FormulaHelper.CalculateActualB2BAvailable,
        // docs/InventoryStateChangedFullQueueTrigger.md §3.4) takes no leverage-percentage input.
        await GetStoreLeverageAsync(itemStockInventory, cancellationToken);

        itemStockInventory.CalculateB2CExtended();

        var newB2CAvailable = itemStockInventory.CalculateB2CAvailable();

        if (newB2CAvailable != prevB2CAvailable)
        {
            deltaResult.IsB2CChanged = true;
            deltaResult.DeltaTowardsOms = newB2CAvailable - prevB2CAvailable;
            itemStockInventory.UpdateB2CAvailable(newB2CAvailable);

            logger.LogInformation(
                "B2C extension recalculated for {ItemStockId}: prevB2CAvail={PrevB2CAvailable}, newB2CAvail={NewB2CAvailable}, delta={Delta}.",
                itemStockInventory.Id, prevB2CAvailable, newB2CAvailable, deltaResult.DeltaTowardsOms);
        }
    }

    /// <summary>
    /// Resolves store leverage item-level-first, fulfilment-level-fallback - mirrors Reflex's
    /// <c>ItemStockInventoryCalculation.GetInventoryStoreLeverageAsync</c> exactly: an item-level rule
    /// is checked first, and only if none exists or it isn't active does the lookup fall back to the
    /// fulfilment-level rule. The resolved percentage itself is not currently consumed by
    /// <see cref="ItemStockInventory.CalculateB2CExtended"/> (see the caller's remark) - this method
    /// exists to preserve the ported lookup order/side effects (e.g. future callers, telemetry) rather
    /// than to feed the formula.
    /// </summary>
    private async Task<decimal> GetStoreLeverageAsync(
        ItemStockInventory itemStockInventory,
        CancellationToken cancellationToken)
    {
        var itemLevelRule = await itemLevelSegmentationRepository.GetItemLevelFulfilmentyByCategory(
            itemStockInventory.FulfilmentId,
            itemStockInventory.Hallmark,
            itemStockInventory.ItemCode,
            itemStockInventory.CountryOfOrigin);

        if (itemLevelRule is { IsActive: true })
        {
            return itemLevelRule.StoreLeveragePercentage ?? 0;
        }

        var fulfilmentLevelRule = await fulfilmentLevelSegmentationRepository.GetFulfilmentLevelFulfilmentyByCategory(
            itemStockInventory.FulfilmentId,
            itemStockInventory.Hallmark,
            cancellationToken);

        return fulfilmentLevelRule?.IsActive == true ? (fulfilmentLevelRule.StoreLeveragePercentage ?? 0) : 0;
    }
}
