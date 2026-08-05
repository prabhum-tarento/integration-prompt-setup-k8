using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Domain.Exceptions;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <inheritdoc cref="IOrderToInventoryAllocatedService"/>
public sealed class OrderToInventoryAllocatedService(
    IItemStockInventoryRepository repository,
    IItemStockInventoryExtensionCalculationService extensionCalculationService,
    IItemStockInventorySegmentationService segmentationService,
    IDomainEventDispatcher domainEventDispatcher,
    TimeProvider timeProvider,
    ILogger<OrderToInventoryAllocatedService> logger) : IOrderToInventoryAllocatedService
{
    private const int MaxConcurrencyRetryAttempts = 3;

    public async Task<ItemStockInventoryDeltaResult> AllocateAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        OrderDomain orderDomain, int allocatedFromB2BBucketQuantity, int allocatedFromB2CBucketQuantity,
        bool isThirdPartyLogistics, CancellationToken cancellationToken = default)
    {
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);
        var deltaResult = new ItemStockInventoryDeltaResult();

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var aggregate = await repository.GetAsync(id, id, cancellationToken);

            if (aggregate is null)
            {
                var exception = new MissingItemStockInventoryException(id, itemCode);
                logger.LogWarning(
                    exception,
                    "MISSING_INVENTORY: no ItemStockInventory record found for {Id} (item {ItemCode}) - skipping allocation.",
                    id, itemCode);

                return deltaResult;
            }

            if (orderDomain != OrderDomain.B2C && allocatedFromB2BBucketQuantity == 0)
            {
                logger.LogWarning(
                    "ORDER_ALLOCATION_SKIPPED: B2BAllocated is zero for {Id} (item {ItemCode}) on domain {OrderDomain} - skipping allocation.",
                    id, itemCode, orderDomain);

                return deltaResult;
            }

            if (orderDomain == OrderDomain.B2C && allocatedFromB2CBucketQuantity != 0)
            {
                var availableB2C = aggregate.IsExtended ? aggregate.B2COriginal : aggregate.B2CAvailable;

                if (aggregate.B2CAllocated + allocatedFromB2CBucketQuantity > availableB2C)
                {
                    logger.LogWarning(
                        "B2C_ALLOCATION_INSUFFICIENT_SOURCE: B2C source {Source} < allocated {Allocated} for {Id} (item {ItemCode}) - skipping allocation.",
                        availableB2C, allocatedFromB2CBucketQuantity, id, itemCode);

                    return deltaResult;
                }
            }

            var prevB2BAllocated = aggregate.B2BAllocated;
            var prevB2CAllocated = aggregate.B2CAllocated;
            var prevB2BUsedShare = aggregate.B2BUsedShare;
            var prevB2CExtended = aggregate.B2CExtended;
            var prevB2CAvailable = aggregate.CalculateB2CAvailable();
            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

            try
            {
                aggregate.AllocateOrder(orderDomain, allocatedFromB2BBucketQuantity, allocatedFromB2CBucketQuantity, nowUtc);

                if (aggregate.IsExtended)
                {
                    await extensionCalculationService.CalculateB2CExtensionAsync(prevB2CAvailable, aggregate, deltaResult, cancellationToken);
                }

                var patchOperations = BuildAllocationPatchOperations(aggregate, prevB2BAllocated, prevB2CAllocated, prevB2BUsedShare, prevB2CExtended);

                await repository.PatchAsync(aggregate.Id, aggregate.Category, aggregate.ETag!, patchOperations, cancellationToken);

                await domainEventDispatcher.DispatchAsync(aggregate.DomainEvents, cancellationToken);

                aggregate.ClearDomainEvents();

                logger.LogInformation(
                    "ORDER_ALLOCATION_APPLIED: {Id} (item {ItemCode}) allocated B2B={B2B} B2C={B2C}, delta={Delta}.",
                    id, itemCode, allocatedFromB2BBucketQuantity, allocatedFromB2CBucketQuantity, deltaResult.DeltaTowardsOms);

                // §3.5: Segmentation/extension step (docs/events/inventory.OrderToInventoryAllocated.md step 7)
                // TODO(ai): unresolved precedence conflict — doc's §2/§6 "step 7 IsItemLevelRuleChanged" vs §3's silence
                // on how this event computes it; implemented here as inboundQty = net allocated per explicit user
                // direction, review before shipping.
                var netAllocatedQuantity = allocatedFromB2BBucketQuantity + allocatedFromB2CBucketQuantity;
                var segmentationResult = await segmentationService.ApplySegmentationAsync(
                    fulfilmentId, itemCode, countryOfOrigin, hallmark,
                    netAllocatedQuantity, isThirdPartyLogistics, cancellationToken);

                if (segmentationResult.IsB2CChanged)
                {
                    deltaResult.IsB2CChanged = true;
                    deltaResult.DeltaTowardsOms += segmentationResult.DeltaTowardsOms;
                }

                return deltaResult;
            }
            catch (InsufficientItemStockException ex)
            {
                logger.LogWarning(
                    ex,
                    "B2C_ALLOCATION_INSUFFICIENT: {Id} (item {ItemCode}) does not have sufficient B2C stock - skipping allocation.",
                    id, itemCode);

                return deltaResult;
            }
            catch (ConcurrencyException ex) when (attempt < MaxConcurrencyRetryAttempts)
            {
                logger.LogWarning(
                    ex,
                    "CONCURRENCY_CONFLICT: allocation for {Id} (item {ItemCode}) failed on attempt {Attempt}, retrying.",
                    id, itemCode, attempt);
            }
        }

        throw new ConcurrencyException(id, "Exhausted retry attempts for order allocation");
    }

    private static IReadOnlyList<PatchOperation> BuildAllocationPatchOperations(
        ItemStockInventory aggregate,
        int prevB2BAllocated,
        int prevB2CAllocated,
        int prevB2BUsedShare,
        int prevB2CExtended)
    {
        var operations = new List<PatchOperation>();

        var b2bAllocatedDelta = aggregate.B2BAllocated - prevB2BAllocated;
        if (b2bAllocatedDelta != 0)
        {
            operations.Add(PatchOperation.Increment("/B2BAllocated", b2bAllocatedDelta));
        }

        var b2cAllocatedDelta = aggregate.B2CAllocated - prevB2CAllocated;
        if (b2cAllocatedDelta != 0)
        {
            operations.Add(PatchOperation.Increment("/B2CAllocated", b2cAllocatedDelta));
        }

        var b2bUsedShareDelta = aggregate.B2BUsedShare - prevB2BUsedShare;
        if (b2bUsedShareDelta != 0)
        {
            operations.Add(PatchOperation.Increment("/B2BUsedShare", b2bUsedShareDelta));
        }

        if (aggregate.IsExtended)
        {
            var b2cExtendedDelta = aggregate.B2CExtended - prevB2CExtended;
            if (b2cExtendedDelta != 0)
            {
                operations.Add(PatchOperation.Increment("/B2CExtended", b2cExtendedDelta));
            }

            var b2cAvailableDelta = aggregate.B2CAvailable - (prevB2CAllocated + prevB2CExtended);
            if (b2cAvailableDelta != 0)
            {
                operations.Add(PatchOperation.Increment("/B2CAvailable", b2cAvailableDelta));
            }
        }

        operations.Add(PatchOperation.Set("/ModifiedUtc", aggregate.ModifiedUtc));

        return operations;
    }
}
