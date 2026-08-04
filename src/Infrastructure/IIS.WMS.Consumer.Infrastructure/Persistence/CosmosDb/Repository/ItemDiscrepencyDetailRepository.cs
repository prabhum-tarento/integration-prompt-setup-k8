using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Shared;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IItemDiscrepencyDetailRepository"/>
public sealed class ItemDiscrepencyDetailRepository : CosmosRepository<ItemDiscrepencyDetail, ItemDiscrepencyDetailDocument>, IItemDiscrepencyDetailRepository
{
    public ItemDiscrepencyDetailRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<ItemDiscrepencyDetailRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.ItemDiscrepencyDetail, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc />
    protected override ItemDiscrepencyDetailDocument ToDocument(ItemDiscrepencyDetail domain) => ItemDiscrepencyDetailMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override ItemDiscrepencyDetail ToDomain(ItemDiscrepencyDetailDocument document) => ItemDiscrepencyDetailMapper.ToDomain(document);
}
