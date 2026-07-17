using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;

/// <summary>
/// One audit persistence destination <see cref="AuditBackgroundService"/> fans a drained
/// <see cref="AuditEntry"/> out to - <see cref="CosmosAuditSink"/> (the <c>AuditLog</c> container) and/or
/// <see cref="ColdBlobAuditSink"/> (the cold-tier Blob Storage archive), gated independently by
/// <see cref="AuditOptions.CosmosDbEnabled"/>/<see cref="AuditOptions.ColdStorageEnabled"/>. Like
/// <see cref="IAuditTrailWriter"/>, Infrastructure-internal - only <see cref="AuditBackgroundService"/>
/// depends on this, so it is not an Application-layer port.
/// </summary>
public interface IAuditSink
{
    /// <summary>
    /// Persists <paramref name="entry"/> to this destination. Never throws - each implementation owns
    /// its own failure handling (retry, dead-letter fallback, or log-and-swallow), so a failure in one
    /// sink never prevents <see cref="AuditBackgroundService"/> from persisting to the others.
    /// </summary>
    Task PersistAsync(AuditEntry entry, CancellationToken cancellationToken = default);
}
