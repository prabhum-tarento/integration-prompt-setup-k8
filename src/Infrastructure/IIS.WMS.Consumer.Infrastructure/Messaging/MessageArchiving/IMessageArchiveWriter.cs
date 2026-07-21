using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;

/// <summary>
/// Non-blocking hand-off to the background <see cref="MessageArchiveBackgroundService"/> - decouples the
/// calling worker's per-message latency from the Cosmos/Blob round-trip a <see cref="MessageArchive"/>
/// persist costs, the same producer/consumer pattern <c>Persistence.CosmosDb.Audit.IAuditTrailWriter</c>
/// and <c>OrderArchiving.IOrderArchiveWriter</c> already use (integration-resiliency.instructions.md §6).
/// </summary>
public interface IMessageArchiveWriter
{
    /// <summary>
    /// Enqueues <paramref name="entry"/> for background persistence. Never throws and never blocks -
    /// a full channel (the background worker falling behind write volume) logs and drops the entry
    /// rather than making the calling worker wait or fail.
    /// </summary>
    void Enqueue(MessageArchive entry);
}
