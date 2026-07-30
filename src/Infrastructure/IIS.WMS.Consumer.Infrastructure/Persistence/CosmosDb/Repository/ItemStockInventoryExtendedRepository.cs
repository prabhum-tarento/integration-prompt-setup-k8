using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Shared;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IItemStockInventoryExtendedRepository"/>
public sealed class ItemStockInventoryExtendedRepository : CosmosRepository<ItemStockInventoryExtended, ItemStockInventoryExtendedDocument>, IItemStockInventoryExtendedRepository
{
    public ItemStockInventoryExtendedRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<ItemStockInventoryExtendedRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc/>
    public Task<ItemStockInventoryExtended?> GetAsync(
        string fulfilmentId, string itemCode, string hallmark, string countryOfOrigin,
        State state, Status status, CancellationToken cancellationToken = default)
    {
        var id = ItemStockInventoryExtended.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin, state, status);
        return GetAsync(id, id, cancellationToken);
    }

    /// <summary>
    /// Resolves the per-fulfilment-code container for <paramref name="category"/> (always an
    /// <see cref="ItemStockInventoryExtended.Id"/> value, whose first
    /// <c>:</c>-delimited segment is the fulfilment code per <see cref="ItemStockInventoryExtended.BuildId"/>) -
    /// this repository has no single container to fall back to, unlike the base class default. Only
    /// correct for the single-item CRUD methods (<c>GetAsync</c>/<c>CreateAsync</c>/<c>ReplaceAsync</c>/etc.),
    /// which always pass the full composite category; <c>GetPagedAsync</c>/<c>QueryAsync</c> instead go
    /// through the <see cref="ResolveContainerName(string?, string?)"/> overload below, since their
    /// caller-supplied <c>QueryOptions.Category</c> carries no such guarantee.
    /// </summary>
    protected override string ResolveContainerName(string? category) =>
        category is null
            ? throw new NotSupportedException(
                $"{nameof(ItemStockInventoryExtendedRepository)} has no single container to scan across " +
                "fulfilment codes - cross-partition queries are not supported.")
            : CosmosContainerNames.GetItemStockInventoryExtendedContainerName(ExtractFulfilmentCode(category));

    /// <summary>
    /// Resolves the per-fulfilment-code container for <c>GetPagedAsync</c>/<c>QueryAsync</c>,
    /// reading <paramref name="fulfilmentCode"/> directly off <c>QueryOptions.FulfilmentCode</c> instead of
    /// parsing it out of <paramref name="category"/> - a paged/projected query's <c>Category</c> is an
    /// arbitrary caller-supplied partition-key filter, not guaranteed to be an
    /// <see cref="ItemStockInventoryExtended.Id"/>-shaped composite key the way the single-item CRUD methods'
    /// <paramref name="category"/> always is.
    /// </summary>
    /// <remarks>
    /// TODO(ai): unresolved pre-existing bug (out of scope for this change, flagged per
    /// CLAUDE.md's precedence-conflict rule since this method is adjacent to code this change
    /// touches) - this calls <see cref="CosmosContainerNames.GetItemStockInventoryContainerName"/>
    /// (the *non-extended* ItemStockInventory container) rather than
    /// <see cref="CosmosContainerNames.GetItemStockInventoryExtendedContainerName"/>. No current
    /// caller exercises <c>GetPagedAsync</c>/<c>QueryAsync</c> on this repository, so it hasn't
    /// surfaced yet.
    /// </remarks>
    protected override string ResolveContainerName(string? category, string? fulfilmentCode) =>
        fulfilmentCode is null
            ? throw new NotSupportedException(
                $"{nameof(ItemStockInventoryExtendedRepository)} requires {nameof(QueryOptions<>.FulfilmentCode)} " +
                "to route a paged/projected query to the correct container - cross-partition queries are not supported.")
            : CosmosContainerNames.GetItemStockInventoryContainerName(fulfilmentCode);

    /// <summary>Extracts the fulfilment code - the first <c>:</c>-delimited segment - from a <see cref="ItemStockInventoryExtended.BuildId"/>-shaped category/id.</summary>
    private static string ExtractFulfilmentCode(string category)
    {
        var separatorIndex = category.IndexOf(':');
        return separatorIndex < 0 ? category : category[..separatorIndex];
    }

    protected override ItemStockInventoryExtendedDocument ToDocument(ItemStockInventoryExtended domain) =>
        ItemStockInventoryExtendedMapper.ToDocument(domain);

    protected override ItemStockInventoryExtended ToDomain(ItemStockInventoryExtendedDocument document) =>
        ItemStockInventoryExtendedMapper.ToDomain(document);
}
