using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.OrderArchiving;

/// <summary>
/// Non-blocking hand-off from <c>KafkaConsumerHostedServiceBase.TryUpsertOrderArchiveAsync</c> to the background
/// <see cref="OrderArchiveBackgroundService"/> - decouples the Kafka worker's per-message latency from
/// the Cosmos round-trip an <see cref="OrderArchive"/> upsert costs, the same producer/consumer pattern
/// <c>AuditTrailWriter</c>/<c>AuditBackgroundService</c> already use for audit-trail writes
/// (integration-resiliency.instructions.md §6).
/// </summary>
public interface IOrderArchiveWriter
{
    /// <summary>
    /// Enqueues <paramref name="entry"/> for background persistence. Never throws and never blocks -
    /// a full channel (the background worker falling behind Cosmos write volume) logs and drops the
    /// entry rather than making the calling worker wait or fail.
    /// </summary>
    void Enqueue(OrderArchive entry);
}
