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
    public AuditRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<AuditRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.AuditLog, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc />
    protected override AuditEntryDocument ToDocument(AuditEntry domain) => AuditEntryMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override AuditEntry ToDomain(AuditEntryDocument document) => AuditEntryMapper.ToDomain(document);
}
