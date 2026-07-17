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
    /// <summary>
    /// Container this repository reads/writes, declared here rather than in shared configuration
    /// (cosmos-db.instructions.md §1) - every other repository declares its own container name the same way.
    /// Provisioned externally (Bicep/Terraform) like every other container - not created by this app.
    /// </summary>
    private const string ContainerName = "OrderArchive";

    public OrderArchiveRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<OrderArchiveRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(ContainerName, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc />
    protected override OrderArchiveDocument ToDocument(OrderArchive domain) => OrderArchiveMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override OrderArchive ToDomain(OrderArchiveDocument document) => OrderArchiveMapper.ToDomain(document);
}
