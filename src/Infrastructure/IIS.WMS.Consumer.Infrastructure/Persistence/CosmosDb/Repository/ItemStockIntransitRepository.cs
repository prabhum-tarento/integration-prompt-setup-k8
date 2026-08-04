using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Shared;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IItemStockIntransitRepository"/>
public sealed class ItemStockIntransitRepository : CosmosRepository<ItemStockIntransit, ItemStockIntransitDocument>, IItemStockIntransitRepository
{
    public ItemStockIntransitRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<ItemStockIntransitRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.ItemStockIntransit, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc />
    protected override ItemStockIntransitDocument ToDocument(ItemStockIntransit domain) => ItemStockIntransitMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override ItemStockIntransit ToDomain(ItemStockIntransitDocument document) => ItemStockIntransitMapper.ToDomain(document);
}
