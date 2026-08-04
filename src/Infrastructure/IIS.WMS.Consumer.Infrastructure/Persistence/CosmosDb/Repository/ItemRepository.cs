using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Shared;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IItemRepository"/>
public sealed class ItemRepository : CosmosRepository<Item, ItemDocument>, IItemRepository
{
    public ItemRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<ItemRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.MasterData, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    private static string Category(string itemCode) => $"Item_{itemCode}";

    /// <inheritdoc/>
    public Task<Item?> GetByItemCodeAsync(string itemCode, CancellationToken cancellationToken = default) =>
        GetAsync(itemCode, Category(itemCode), cancellationToken);

    protected override ItemDocument ToDocument(Item domain) => ItemMapper.ToDocument(domain);

    protected override Item ToDomain(ItemDocument document) => ItemMapper.ToDomain(document);
}
