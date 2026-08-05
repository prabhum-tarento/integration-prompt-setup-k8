using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Exceptions;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <inheritdoc cref="IConsolidatedOrderShippedService"/>
public sealed class ConsolidatedOrderShippedService(
    IItemStockInventoryRepository inventoryRepository,
    IItemStockInventoryExtensionCalculationService extensionCalculationService,
    IDomainEventDispatcher domainEventDispatcher,
    TimeProvider timeProvider,
    ILogger<ConsolidatedOrderShippedService> logger) : IConsolidatedOrderShippedService
{
    private const int MaxConcurrencyRetryAttempts = 3;

    public async Task<ItemStockInventoryDeltaResult> ConfirmAsync(
        B2BOrderConfirmedRequest request, CancellationToken cancellationToken = default)
    {
        var id = ItemStockInventory.BuildId(request.FulfilmentCode, request.ItemCode, request.Hallmark, request.CountryOfOrigin);
        var deltaResult = new ItemStockInventoryDeltaResult();

        if (request.ShippedQuantity <= 0)
        {
            logger.LogWarning(
                "INVALID_QUANTITY: ShippedQuantity {ShippedQuantity} is not positive for {Id} (item {ItemCode}) - skipping confirmation.",
                request.ShippedQuantity, id, request.ItemCode);

            return deltaResult;
        }

        if (request.AllocatedFromB2BBucketQuantity < request.ShippedQuantity)
        {
            logger.LogWarning(
                "INVALID_ALLOCATION: AllocatedFromB2BBucketQuantity {Allocated} is less than ShippedQuantity {Shipped} for {Id} (item {ItemCode}) - skipping confirmation.",
                request.AllocatedFromB2BBucketQuantity, request.ShippedQuantity, id, request.ItemCode);

            return deltaResult;
        }

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var aggregate = await inventoryRepository.GetAsync(id, id, cancellationToken);

            if (aggregate is null)
            {
                var exception = new MissingItemStockInventoryException(id, request.ItemCode);
                logger.LogWarning(
                    exception,
                    "MISSING_INVENTORY: no ItemStockInventory record found for {Id} (item {ItemCode}) - skipping confirmation.",
                    id, request.ItemCode);

                return deltaResult;
            }

            var prevB2BAvailable = aggregate.B2BAvailable;
            var prevB2BPrepared = aggregate.B2BPrepared;
            var prevPsc = aggregate.Psc;
            var prevB2CExtended = aggregate.B2CExtended;
            var prevB2CAvailable = aggregate.B2CAvailable;
            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

            aggregate.ApplyConsolidatedShipment(request.ConfirmationType, request.ShippedQuantity, nowUtc);

            if (aggregate.IsExtended)
            {
                await extensionCalculationService.CalculateB2CExtensionAsync(prevB2CAvailable, aggregate, deltaResult, cancellationToken);
            }

            List<PatchOperation> operations =
            [
                PatchOperation.Increment("/B2BAVL", aggregate.B2BAvailable - prevB2BAvailable),
                PatchOperation.Increment("/B2BPrepared", aggregate.B2BPrepared - prevB2BPrepared),
                PatchOperation.Increment("/PSC", aggregate.Psc - prevPsc),
                PatchOperation.Set("/Timestamp", aggregate.ModifiedUtc.ToString("O")),
            ];

            if (aggregate.IsExtended)
            {
                operations.Add(PatchOperation.Increment("/B2CExtended", aggregate.B2CExtended - prevB2CExtended));
                operations.Add(PatchOperation.Increment("/B2CAVL", aggregate.B2CAvailable - prevB2CAvailable));
            }

            try
            {
                await inventoryRepository.PatchAsync(
                    aggregate.Id, aggregate.Category, aggregate.ETag!, operations, cancellationToken);
                await domainEventDispatcher.DispatchAsync(aggregate.DomainEvents, cancellationToken);

                aggregate.ClearDomainEvents();

                if (aggregate.B2CAvailable != prevB2CAvailable)
                {
                    deltaResult.IsB2CChanged = true;
                    deltaResult.DeltaTowardsOms = aggregate.B2CAvailable - prevB2CAvailable;
                }

                logger.LogInformation(
                    "CONSOLIDATED_ORDER_SHIPPED_APPLIED: {Id} (item {ItemCode}) confirmed {ConfirmationType} quantity={Quantity}, delta={Delta}.",
                    id, request.ItemCode, request.ConfirmationType, request.ShippedQuantity, deltaResult.DeltaTowardsOms);

                return deltaResult;
            }
            catch (ConcurrencyException) when (attempt < MaxConcurrencyRetryAttempts)
            {
                logger.LogWarning(
                    "CONCURRENCY_CONFLICT: confirmation for {Id} (item {ItemCode}) failed on attempt {Attempt}, retrying.",
                    id, request.ItemCode, attempt);
            }
        }

        throw new ConcurrencyException(id, "unknown");
    }
}
