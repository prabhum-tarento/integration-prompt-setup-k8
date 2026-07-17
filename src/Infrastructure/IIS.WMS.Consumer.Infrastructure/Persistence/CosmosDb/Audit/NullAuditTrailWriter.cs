using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;

/// <summary>
/// No-op <see cref="IAuditTrailWriter"/> used only for <c>AuditRepository</c>'s own base-class
/// dependency (see <c>AuditServiceCollectionExtensions</c>) - persisting an audit record must not
/// itself enqueue another audit record. Every other repository gets the real <see cref="AuditTrailWriter"/>.
/// </summary>
public sealed class NullAuditTrailWriter : IAuditTrailWriter
{
    public static readonly NullAuditTrailWriter Instance = new();

    private NullAuditTrailWriter()
    {
    }

    /// <inheritdoc />
    public void Enqueue(AuditEntry entry)
    {
    }
}
