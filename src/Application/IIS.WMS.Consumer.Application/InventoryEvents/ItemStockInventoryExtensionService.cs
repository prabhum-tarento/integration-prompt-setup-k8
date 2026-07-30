using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Exceptions;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Wrapper around <see cref="IItemStockInventoryService"/> that additionally handles B2C extension
/// recalculation and OMS delta metrics post-pick/unpick - ported from Reflex's orchestrator logic.
/// </summary>
public sealed class ItemStockInventoryExtensionService(
    IItemStockInventoryRepository repository,
    IItemStockInventoryService itemStockInventoryService,
    IItemStockInventoryExtensionCalculationService extensionCalculationService,
    ILogger<ItemStockInventoryExtensionService> logger) : IItemStockInventoryExtensionService
{
    /// <inheritdoc />
    public async Task<ItemStockInventoryDeltaResult> ApplyPickB2BWithExtensionAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default)
    {
        var deltaResult = new ItemStockInventoryDeltaResult();

        await itemStockInventoryService.ApplyPickAsync(
            fulfilmentId, itemCode, countryOfOrigin, hallmark,
            ItemStockPickChannel.B2B, quantity, cancellationToken);

        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);
        var aggregate = await repository.GetAsync(id, id, cancellationToken);

        if (aggregate?.IsExtended == true)
        {
            var prevB2CAvailable = aggregate.B2CAvailable;
            var prevB2CExtended = aggregate.B2CExtended;
            await extensionCalculationService.CalculateB2CExtensionAsync(
                prevB2CAvailable, aggregate, deltaResult, cancellationToken);

            if (deltaResult.IsB2CChanged)
            {
                await repository.PatchAsync(
                    aggregate.Id, aggregate.Category, aggregate.ETag!,
                    [
                        PatchOperation.Increment("/B2CExtended", aggregate.B2CExtended - prevB2CExtended),
                        PatchOperation.Increment("/B2CAVL", aggregate.B2CAvailable - prevB2CAvailable),
                    ],
                    cancellationToken);
                logger.LogInformation(
                    "Applied B2B pick with extension recalculation for {Id}: delta={Delta}.", id, deltaResult.DeltaTowardsOms);
            }
        }

        return deltaResult;
    }

    /// <inheritdoc />
    public async Task<ItemStockInventoryDeltaResult> ApplyPickB2CWithExtensionAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default)
    {
        var deltaResult = new ItemStockInventoryDeltaResult();

        await itemStockInventoryService.ApplyPickAsync(
            fulfilmentId, itemCode, countryOfOrigin, hallmark,
            ItemStockPickChannel.B2C, quantity, cancellationToken);

        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);
        var aggregate = await repository.GetAsync(id, id, cancellationToken);

        if (aggregate?.IsExtended == true)
        {
            var prevB2CAvailable = aggregate.B2CAvailable;
            var prevB2CExtended = aggregate.B2CExtended;
            await extensionCalculationService.CalculateB2CExtensionAsync(
                prevB2CAvailable, aggregate, deltaResult, cancellationToken);

            if (deltaResult.IsB2CChanged)
            {
                await repository.PatchAsync(
                    aggregate.Id, aggregate.Category, aggregate.ETag!,
                    [
                        PatchOperation.Increment("/B2CExtended", aggregate.B2CExtended - prevB2CExtended),
                        PatchOperation.Increment("/B2CAVL", aggregate.B2CAvailable - prevB2CAvailable),
                    ],
                    cancellationToken);
                logger.LogInformation(
                    "Applied B2C pick with extension recalculation for {Id}: delta={Delta}.", id, deltaResult.DeltaTowardsOms);
            }
        }

        return deltaResult;
    }

    /// <inheritdoc />
    public async Task<ItemStockInventoryDeltaResult> ApplyUnpickWithExtensionAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default)
    {
        var deltaResult = new ItemStockInventoryDeltaResult();

        await itemStockInventoryService.ApplyUnpickAsync(
            fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity, cancellationToken);

        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);
        var aggregate = await repository.GetAsync(id, id, cancellationToken);

        if (aggregate?.IsExtended == true)
        {
            var prevB2CAvailable = aggregate.B2CAvailable;
            var prevB2CExtended = aggregate.B2CExtended;
            await extensionCalculationService.CalculateB2CExtensionAsync(
                prevB2CAvailable, aggregate, deltaResult, cancellationToken);

            if (deltaResult.IsB2CChanged)
            {
                await repository.PatchAsync(
                    aggregate.Id, aggregate.Category, aggregate.ETag!,
                    [
                        PatchOperation.Increment("/B2CExtended", aggregate.B2CExtended - prevB2CExtended),
                        PatchOperation.Increment("/B2CAVL", aggregate.B2CAvailable - prevB2CAvailable),
                    ],
                    cancellationToken);
                logger.LogInformation(
                    "Applied unpick with extension recalculation for {Id}: delta={Delta}.", id, deltaResult.DeltaTowardsOms);
            }
        }

        return deltaResult;
    }
}
