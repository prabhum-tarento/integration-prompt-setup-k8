using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IItemStockInventoryRepository"/>
public sealed class ItemStockInventoryRepository : CosmosRepository<ItemStockInventory, ItemStockInventoryDocument>, IItemStockInventoryRepository
{
    public ItemStockInventoryRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<ItemStockInventoryRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <summary>
    /// Resolves the per-fulfilment-code container for <paramref name="category"/> (always an
    /// <see cref="ItemStockInventory.Category"/> value, whose first
    /// <c>:</c>-delimited segment is the fulfilment code per <see cref="ItemStockInventory.BuildId"/>) -
    /// this repository has no single container to fall back to, unlike the base class default. Only
    /// correct for the single-item CRUD methods (<c>GetAsync</c>/<c>CreateAsync</c>/<c>ReplaceAsync</c>/etc.),
    /// which always pass the full composite category; <c>GetPagedAsync</c>/<c>QueryAsync</c> instead go
    /// through the <see cref="ResolveContainerName(string?, string?)"/> overload below, since their
    /// caller-supplied <c>QueryOptions.Category</c> carries no such guarantee.
    /// </summary>
    protected override string ResolveContainerName(string? category) =>
        category is null
            ? throw new NotSupportedException(
                $"{nameof(ItemStockInventoryRepository)} has no single container to scan across " +
                "fulfilment codes - cross-partition queries are not supported.")
            : CosmosContainerNames.GetItemStockInventoryContainerName(ExtractFulfilmentCode(category));

    /// <summary>
    /// Resolves the per-fulfilment-code container for <c>GetPagedAsync</c>/<c>QueryAsync</c>,
    /// reading <paramref name="fulfilmentCode"/> directly off <c>QueryOptions.FulfilmentCode</c> instead of
    /// parsing it out of <paramref name="category"/> - a paged/projected query's <c>Category</c> is an
    /// arbitrary caller-supplied partition-key filter, not guaranteed to be an
    /// <see cref="ItemStockInventory.BuildId"/>-shaped composite key the way the single-item CRUD methods'
    /// <paramref name="category"/> always is.
    /// </summary>
    protected override string ResolveContainerName(string? category, string? fulfilmentCode) =>
        fulfilmentCode is null
            ? throw new NotSupportedException(
                $"{nameof(ItemStockInventoryRepository)} requires {nameof(QueryOptions<>.FulfilmentCode)} " +
                "to route a paged/projected query to the correct container - cross-partition queries are not supported.")
            : CosmosContainerNames.GetItemStockInventoryContainerName(fulfilmentCode);

    /// <summary>Extracts the fulfilment code - the first <c>:</c>-delimited segment - from a <see cref="ItemStockInventory.BuildId"/>-shaped category/id.</summary>
    private static string ExtractFulfilmentCode(string category)
    {
        var separatorIndex = category.IndexOf(':');
        return separatorIndex < 0 ? category : category[..separatorIndex];
    }

    /// <inheritdoc />
    protected override ItemStockInventoryDocument ToDocument(ItemStockInventory domain) => ItemStockInventoryMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override ItemStockInventory ToDomain(ItemStockInventoryDocument document) => ItemStockInventoryMapper.ToDomain(document);
}
