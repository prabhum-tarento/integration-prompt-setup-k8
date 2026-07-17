using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// Port for audit-trail persistence (cosmos-db.instructions.md §5), implemented by
/// <c>Infrastructure.Persistence.CosmosDb.Repository.AuditRepository</c>. Consumed only by
/// <c>Infrastructure.Persistence.CosmosDb.Audit.AuditBackgroundService</c>, which drains the
/// in-memory audit channel and persists each entry through here - never called directly from a
/// controller or Application use case.
/// </summary>
public interface IAuditRepository
{
    /// <summary>
    /// Persists a new audit record. Every <see cref="AuditEntry.Id"/> already carries a fresh GUID
    /// suffix (see <see cref="AuditEntry.Create"/>'s remarks), so a redelivered/retried mutation
    /// producing a second audit record is expected - never a conflict to resolve.
    /// </summary>
    Task<AuditEntry> CreateAsync(AuditEntry entry, CancellationToken cancellationToken = default);
}
