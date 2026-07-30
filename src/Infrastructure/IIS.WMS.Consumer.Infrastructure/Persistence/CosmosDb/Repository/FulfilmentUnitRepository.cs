using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Shared;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IFulfilmentUnitRepository"/>
public sealed class FulfilmentUnitRepository : CosmosRepository<FulfilmentUnit, FulfilmentUnitDocument>, IFulfilmentUnitRepository
{
    public FulfilmentUnitRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<FulfilmentUnitRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.MasterData, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    private static string Category(string fulfilmentId) => $"FU_{fulfilmentId}";

    /// <inheritdoc/>
    public Task<FulfilmentUnit?> GetByFulfilmentIdAsync(string fulfilmentId, CancellationToken cancellationToken = default) =>
        GetAsync(fulfilmentId, Category(fulfilmentId), cancellationToken);

    protected override FulfilmentUnitDocument ToDocument(FulfilmentUnit domain) => FulfilmentUnitMapper.ToDocument(domain);

    protected override FulfilmentUnit ToDomain(FulfilmentUnitDocument document) => FulfilmentUnitMapper.ToDomain(document);
}
