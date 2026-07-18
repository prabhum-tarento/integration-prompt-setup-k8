using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;

/// <inheritdoc cref="IAuditTrailWriter"/>
/// <remarks>
/// Wraps the bounded <see cref="Channel{T}"/> registered by <c>AuditServiceCollectionExtensions</c> -
/// the same producer/consumer-decoupling pattern <c>ConsumerHostedService</c> uses for its own worker
/// pool (integration-resiliency.instructions.md §6), here decoupling a Cosmos mutation from the
/// (slower, Cosmos-round-trip-bound) audit write. <see cref="Enqueue"/> uses <see cref="ChannelWriter{T}.TryWrite"/>,
/// never <c>WriteAsync</c> - <c>TryWrite</c> never blocks, which is what makes audit capture genuinely
/// non-blocking for the calling mutation rather than merely "usually fast."
/// </remarks>
public sealed class AuditTrailWriter(
    Channel<AuditEntry> channel, IOptions<AuditOptions> options, ILogger<AuditTrailWriter> logger) : IAuditTrailWriter
{
    /// <summary><see cref="AuditOptions.ExcludedContainers"/>, indexed for a case-insensitive lookup per <see cref="Enqueue"/> call.</summary>
    private readonly HashSet<string> excludedContainers = new(options.Value.ExcludedContainers, StringComparer.OrdinalIgnoreCase);

    /// <inheritdoc />
    public void Enqueue(AuditEntry entry)
    {
        if (excludedContainers.Contains(entry.ContainerName))
        {
            return;
        }

        if (channel.Writer.TryWrite(entry))
        {
            return;
        }

        // The channel is full - the audit background worker is falling behind Cosmos write volume
        // (or the process is shutting down and the writer has already been completed). Dropping here
        // rather than awaiting keeps the guarantee that a mutation is never slowed down by auditing;
        // this Critical log is the operator-facing signal that Audit:ChannelCapacity needs raising or
        // the Audit container's throughput needs investigating, per this repo's own "never block, but
        // never silently swallow" philosophy (integration-resiliency.instructions.md §1/§5).
        logger.LogCritical(
            "Audit channel is full or closed - dropping audit entry {Id} for {ContainerName}/{EntityId} " +
            "(operation {Operation}, correlation id {CorrelationId}).",
            entry.Id, entry.ContainerName, entry.EntityId, entry.Operation, entry.CorrelationId);
    }
}
