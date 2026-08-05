using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <inheritdoc cref="IItemStockWarehouseInventoryService"/>
public sealed class ItemStockWarehouseInventoryService(
    IItemStockWarehouseInventoryRepository repository,
    TimeProvider timeProvider,
    ILogger<ItemStockWarehouseInventoryService> logger) : IItemStockWarehouseInventoryService
{
    private const int MaxConcurrencyRetryAttempts = 3;

    public async Task ApplyShipmentAsync(
        string fulfilmentId, string itemCode, int shippedQuantity, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shippedQuantity);

        var id = ItemStockWarehouseInventory.BuildId(fulfilmentId, itemCode);
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var existing = await repository.GetAsync(id, id, cancellationToken);

            if (existing is null)
            {
                var created = ItemStockWarehouseInventory.CreateDefault(fulfilmentId, itemCode, shippedQuantity, nowUtc);
                await repository.CreateAsync(created, cancellationToken);

                logger.LogInformation(
                    "ITEM_STOCK_WAREHOUSE_INVENTORY_CREATED: {Id} created with quantity={Quantity}.",
                    id, shippedQuantity);

                return;
            }

            List<PatchOperation> operations =
            [
                PatchOperation.Increment("/Qnty", shippedQuantity),
                PatchOperation.Set("/ModifiedUtc", nowUtc.ToString("O")),
            ];

            try
            {
                await repository.PatchAsync(existing.Id, existing.Category, existing.ETag!, operations, cancellationToken);

                logger.LogInformation(
                    "ITEM_STOCK_WAREHOUSE_INVENTORY_INCREMENTED: {Id} incremented by quantity={Quantity}.",
                    id, shippedQuantity);

                return;
            }
            catch (ConcurrencyException) when (attempt < MaxConcurrencyRetryAttempts)
            {
                logger.LogWarning(
                    "CONCURRENCY_CONFLICT: warehouse-stock increment for {Id} failed on attempt {Attempt}, retrying.",
                    id, attempt);
            }
        }

        throw new ConcurrencyException(id, "unknown");
    }
}
