using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Shared;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="ISnapshotStockSyncItemRepository"/>
public sealed class SnapshotStockSyncItemRepository : CosmosRepository<SnapshotStockSyncItem, SnapshotStockSyncItemDocument>, ISnapshotStockSyncItemRepository
{
    public SnapshotStockSyncItemRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<SnapshotStockSyncItemRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.SnapshotStockSyncItem, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc />
    protected override SnapshotStockSyncItemDocument ToDocument(SnapshotStockSyncItem domain) => SnapshotStockSyncItemMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override SnapshotStockSyncItem ToDomain(SnapshotStockSyncItemDocument document) => SnapshotStockSyncItemMapper.ToDomain(document);
}
