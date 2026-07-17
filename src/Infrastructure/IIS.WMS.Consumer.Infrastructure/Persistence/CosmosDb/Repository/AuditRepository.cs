using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IAuditRepository"/>
/// <remarks>
/// Always constructed with <see cref="NullAuditTrailWriter.Instance"/> (see
/// <c>AuditServiceCollectionExtensions</c>), never the real <see cref="IAuditTrailWriter"/> singleton -
/// otherwise persisting an audit record would itself enqueue another audit record, forever.
/// </remarks>
public sealed class AuditRepository : CosmosRepository<AuditEntry, AuditEntryDocument>, IAuditRepository
{
    /// <summary>
    /// Container this repository writes to, declared here rather than in shared configuration
    /// (cosmos-db.instructions.md §1) - every other repository declares its own container name the same way.
    /// Provisioned externally (Bicep/Terraform) like every other container - not created by this app.
    /// </summary>
    private const string ContainerName = "AuditLog";

    public AuditRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<AuditRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(ContainerName, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc />
    protected override AuditEntryDocument ToDocument(AuditEntry domain) => AuditEntryMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override AuditEntry ToDomain(AuditEntryDocument document) => AuditEntryMapper.ToDomain(document);
}
