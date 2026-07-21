using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IOrderArchiveRepository"/>
public sealed class OrderArchiveRepository : CosmosRepository<OrderArchive, OrderArchiveDocument>, IOrderArchiveRepository
{
    public OrderArchiveRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<OrderArchiveRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.OrderArchive, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc />
    protected override OrderArchiveDocument ToDocument(OrderArchive domain) => OrderArchiveMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override OrderArchive ToDomain(OrderArchiveDocument document) => OrderArchiveMapper.ToDomain(document);
}
