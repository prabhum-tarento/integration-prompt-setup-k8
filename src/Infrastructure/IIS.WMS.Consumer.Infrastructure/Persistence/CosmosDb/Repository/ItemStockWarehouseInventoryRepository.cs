using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Shared;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IItemStockWarehouseInventoryRepository"/>
public sealed class ItemStockWarehouseInventoryRepository
    : CosmosRepository<ItemStockWarehouseInventory, ItemStockWarehouseInventoryDocument>, IItemStockWarehouseInventoryRepository
{
    public ItemStockWarehouseInventoryRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<ItemStockWarehouseInventoryRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <summary>
    /// Resolves the per-fulfilment-code container for <paramref name="category"/> (always an
    /// <see cref="ItemStockWarehouseInventory.Category"/> value, whose first <c>:</c>-delimited
    /// segment is the fulfilment code per <see cref="ItemStockWarehouseInventory.BuildId"/>) -
    /// mirrors <see cref="ItemStockInventoryRepository"/>.
    /// </summary>
    protected override string ResolveContainerName(string? category) =>
        category is null
            ? throw new NotSupportedException(
                $"{nameof(ItemStockWarehouseInventoryRepository)} has no single container to scan across " +
                "fulfilment codes - cross-partition queries are not supported.")
            : CosmosContainerNames.GetItemStockWarehouseInventoryContainerName(ExtractFulfilmentCode(category));

    /// <summary>Extracts the fulfilment code - the first <c>:</c>-delimited segment - from an <see cref="ItemStockWarehouseInventory.BuildId"/>-shaped category/id.</summary>
    private static string ExtractFulfilmentCode(string category)
    {
        var separatorIndex = category.IndexOf(':');
        return separatorIndex < 0 ? category : category[..separatorIndex];
    }

    /// <inheritdoc />
    protected override ItemStockWarehouseInventoryDocument ToDocument(ItemStockWarehouseInventory domain) =>
        ItemStockWarehouseInventoryMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override ItemStockWarehouseInventory ToDomain(ItemStockWarehouseInventoryDocument document) =>
        ItemStockWarehouseInventoryMapper.ToDomain(document);
}
