using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;

/// <summary>
/// Non-blocking hand-off from a <c>CosmosRepository{TDomain,TDocument}</c> mutation to the background
/// audit pipeline (<see cref="AuditBackgroundService"/>) - the mechanism that lets every add/edit/delete
/// across every repository be audited without slowing down (or ever failing) the mutation itself.
/// Infrastructure-internal: only <c>CosmosRepository{TDomain,TDocument}</c> depends on this, so unlike
/// <c>IAuditRepository</c> it is not an Application-layer port.
/// </summary>
public interface IAuditTrailWriter
{
    /// <summary>
    /// Enqueues <paramref name="entry"/> for background persistence. Never throws and never blocks -
    /// a full channel (the audit background worker falling behind Cosmos write volume) logs and drops
    /// the entry rather than making the caller wait or fail.
    /// </summary>
    void Enqueue(AuditEntry entry);
}
