using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <inheritdoc cref="IItemStockInventorySegmentationService"/>
public sealed class ItemStockInventorySegmentationService(
    IItemStockInventoryRepository repository,
    IItemLevelSegmentationRepository itemLevelSegmentationRepository,
    TimeProvider timeProvider,
    ILogger<ItemStockInventorySegmentationService> logger) : IItemStockInventorySegmentationService
{
    private const int MaxConcurrencyRetryAttempts = 3;

    /// <summary>
    /// The upstream Reflex facade's <c>updateItemLevelSegmentationHandlerAsync</c> gate
    /// (<c>ReflexConstants.TDCFulfilmentId</c>) - kept as a local literal rather than a reference to
    /// <c>Infrastructure.Messaging.Events.InventoryStateChanged.FulfilmentLocationIds.Tdc</c>, since the
    /// Application layer must not depend on Infrastructure types
    /// (dotnet-architecture-good-practices.instructions.md).
    /// </summary>
    private const string TdcFulfilmentId = "TDC";

    /// <inheritdoc />
    public async Task<ItemStockInventoryDeltaResult> ApplySegmentationAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int inboundQty, bool isThirdPartyLogistics, CancellationToken cancellationToken = default)
    {
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);
        var deltaResult = new ItemStockInventoryDeltaResult();

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var aggregate = await repository.GetAsync(id, id, cancellationToken);
            var wasCreated = aggregate is null;
            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

            aggregate ??= ItemStockInventory.CreateDefault(fulfilmentId, itemCode, hallmark, countryOfOrigin, nowUtc);

            if (inboundQty < 0 && wasCreated)
            {
                logger.LogWarning(
                    "Stock inventory {Id} is zero and cannot be negated by inbound quantity {InboundQty} - skipping segmentation.",
                    id, inboundQty);

                return deltaResult;
            }

            var prevB2CAvailable = aggregate.B2CAvailable;
            var prevB2BAvailable = aggregate.B2BAvailable;
            var prevB2COriginal = aggregate.B2COriginal;
            var prevB2CExtended = aggregate.B2CExtended;
            var isB2CChanged = false;
            List<PatchOperation> patchOperations;

            if (isThirdPartyLogistics)
            {
                aggregate.DoFulfilmentLevelB2CSegmentation(inboundQty, nowUtc);
                isB2CChanged = true;
                patchOperations =
                [
                    PatchOperation.Increment("/B2CAVL", aggregate.B2CAvailable - prevB2CAvailable),
                    PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
                ];
            }
            else
            {
                var itemLevelRule = await itemLevelSegmentationRepository.GetItemLevelFulfilmentyByCategory(
                    fulfilmentId, hallmark, itemCode, countryOfOrigin);

                if (itemLevelRule is { IsActive: true })
                {
                    aggregate.ActivateExtension();
                    aggregate.DoItemLevelExtension(inboundQty, itemLevelRule.EcomShare ?? 0, nowUtc);
                    isB2CChanged = true;
                    patchOperations =
                    [
                        PatchOperation.Set("/IsExtended", aggregate.IsExtended),
                        PatchOperation.Increment("/B2COrg", aggregate.B2COriginal - prevB2COriginal),
                        PatchOperation.Increment("/B2BAVL", aggregate.B2BAvailable - prevB2BAvailable),
                        PatchOperation.Increment("/B2CExtended", aggregate.B2CExtended - prevB2CExtended),
                        PatchOperation.Increment("/B2CAVL", aggregate.B2CAvailable - prevB2CAvailable),
                        PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
                    ];
                }
                else
                {
                    aggregate.DoFulfilmentLevelSegmentation(inboundQty, nowUtc);
                    patchOperations =
                    [
                        PatchOperation.Increment("/B2BAVL", aggregate.B2BAvailable - prevB2BAvailable),
                        PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
                    ];
                }
            }

            if (isB2CChanged)
            {
                deltaResult.IsB2CChanged = true;
                deltaResult.DeltaTowardsOms = aggregate.B2CAvailable - prevB2CAvailable;
            }

            try
            {
                if (wasCreated)
                {
                    await repository.CreateAsync(aggregate, cancellationToken);
                }
                else
                {
                    await repository.PatchAsync(
                        aggregate.Id, aggregate.Category, aggregate.ETag!, patchOperations, cancellationToken);
                }

                if (fulfilmentId != TdcFulfilmentId)
                {
                    await UpdateItemLevelSegmentationAsync(aggregate, cancellationToken);
                }

                logger.LogInformation(
                    "Applied §3.3 segmentation to ItemStockInventory {Id}: inboundQty={InboundQty}, delta={Delta}.",
                    id, inboundQty, deltaResult.DeltaTowardsOms);

                return deltaResult;
            }
            catch (ConcurrencyException) when (attempt < MaxConcurrencyRetryAttempts && !wasCreated)
            {
                logger.LogWarning(
                    "Concurrency conflict applying segmentation to {Id}, attempt {Attempt}/{MaxAttempts} - retrying.",
                    id, attempt, MaxConcurrencyRetryAttempts);
            }
        }

        throw new ConcurrencyException(id, "unknown");
    }

    /// <summary>
    /// §3.3 item-level segmentation rule write-back (docs/events/inventory.InventoryStateChanged.md) -
    /// mirrors the upstream Reflex facade's <c>ItemLevelSegmentationRepository.UpdateItemLevelFulfilmentAsync</c>
    /// exactly, including its unconditional <c>IsExtended = true</c> flip (Reflex hardcodes this
    /// regardless of the inventory record's own <see cref="ItemStockInventory.IsExtended"/> value). A
    /// no-op (logged, not an error) if no matching rule exists.
    /// </summary>
    private async Task UpdateItemLevelSegmentationAsync(ItemStockInventory aggregate, CancellationToken cancellationToken)
    {
        var rule = await itemLevelSegmentationRepository.GetItemLevelFulfilmentyByCategory(
            aggregate.FulfilmentId, aggregate.Hallmark, aggregate.ItemCode, aggregate.CountryOfOrigin);

        if (rule is null)
        {
            logger.LogInformation(
                "No item-level segmentation rule found for {Id} - skipping write-back.", aggregate.Id);

            return;
        }

        rule.IsExtended = true;
        rule.CurrentOmniStock = aggregate.B2BAvailable + (aggregate.IsExtended ? aggregate.B2COriginal : aggregate.B2CAvailable);
        rule.CurrentEcomStock = aggregate.B2CAvailable;
        rule.StoreShare = aggregate.B2BAvailable;
        rule.InTransit = aggregate.InTransit;
        rule.LastModified = timeProvider.GetUtcNow().UtcDateTime;

        await itemLevelSegmentationRepository.UpdateItemLevelFulfilmentAsync(rule, cancellationToken);
    }
}
