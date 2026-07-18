using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog.Context;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;

/// <summary>
/// Drains the bounded audit <see cref="Channel{T}"/> <see cref="AuditTrailWriter"/> enqueues onto and
/// fans each entry out to every registered <see cref="IAuditSink"/> - the background half of the
/// non-blocking audit pipeline (integration-resiliency.instructions.md §6's Channel pattern). Which
/// sink(s) are registered depends on <see cref="AuditOptions.CosmosDbEnabled"/>/
/// <see cref="AuditOptions.ColdStorageEnabled"/> (see <c>AuditServiceCollectionExtensions.AddAuditTrail</c>) -
/// this class itself has no knowledge of Cosmos or Blob Storage.
/// </summary>
/// <remarks>
/// <para>
/// <b>Durability across a restart/redeploy.</b> <see cref="StopAsync"/> completes the channel's writer
/// instead of relying on <see cref="BackgroundService"/>'s default cancellation-token behavior, and
/// <see cref="ExecuteAsync"/> deliberately reads with <see cref="CancellationToken.None"/> rather than
/// the stopping token - so on a graceful shutdown (SIGTERM from a rolling deployment, pod restart, or
/// scale-down, within <c>terminationGracePeriodSeconds</c>/the host's shutdown timeout -
/// kubernetes-deployment-best-practices.instructions.md), every entry already buffered in the channel is
/// still drained and persisted before the process exits, not abandoned mid-flight.
/// </para>
/// <para>
/// <b>What this does not cover.</b> A hard crash - SIGKILL, OOM-kill, a host power loss - runs no
/// shutdown code at all, so whatever is buffered in the channel at that instant is lost; this is an
/// inherent limitation of any in-memory buffer, not something closeable without a durably-persisted
/// queue ahead of it (e.g. writing straight to Service Bus/Kafka first, which is a larger change than
/// this pipeline). The same trade-off is already accepted elsewhere in this codebase for the same
/// reason - see <c>KafkaConsumerHostedServiceBase</c>'s <c>ServiceBusSender</c> disposal remarks. Each
/// <see cref="IAuditSink"/> owns its own failure handling for its own destination (e.g.
/// <see cref="CosmosAuditSink"/>'s hot-tier dead-letter fallback) - a sink's own write failure is never
/// this class's concern.
/// </para>
/// </remarks>
public sealed class AuditBackgroundService(
    Channel<AuditEntry> channel,
    IServiceScopeFactory scopeFactory,
    ILogger<AuditBackgroundService> logger) : BackgroundService
{
    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Audit background worker starting.");

        // CancellationToken.None, not stoppingToken: see this class's remarks on graceful-shutdown
        // draining. StopAsync below completes the channel writer, which is what ends this enumeration
        // once every buffered entry has been read - not cancellation.
        await foreach (var entry in channel.Reader.ReadAllAsync(CancellationToken.None))
        {
            await PersistAsync(entry, CancellationToken.None);
        }

        logger.LogInformation("Audit background worker drained the channel and is stopping.");
    }

    /// <inheritdoc />
    /// <remarks>
    /// Completing the writer here - before awaiting <see cref="BackgroundService.StopAsync"/> - is what
    /// bounds the drain to the host's own shutdown timeout instead of running forever: once completed,
    /// <see cref="ChannelReader{T}.ReadAllAsync"/> in <see cref="ExecuteAsync"/> ends naturally after the
    /// last buffered item, rather than waiting indefinitely for a next write that will never come.
    /// </remarks>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        channel.Writer.TryComplete();
        await base.StopAsync(cancellationToken);
    }

    /// <summary>
    /// Fans one entry out to every registered <see cref="IAuditSink"/>, through a fresh DI scope -
    /// <see cref="IAuditSink"/> implementations are Scoped (they depend on Scoped services like
    /// <see cref="IAuditRepository"/>, cosmos-db.instructions.md §12), so this singleton
    /// <see cref="BackgroundService"/> cannot hold them directly. Each sink is contracted to never
    /// throw (see <see cref="IAuditSink.PersistAsync"/>), so one sink's failure never prevents the
    /// others from running. Pushes <see cref="AuditEntry.CorrelationId"/> onto Serilog's ambient
    /// <see cref="LogContext"/> (integration-resiliency.instructions.md §7) so every log line emitted
    /// while persisting - including inside the sinks themselves - carries it, the same convention
    /// <c>KafkaConsumerHostedServiceBase</c> uses at the Kafka boundary.
    /// </summary>
    private async Task PersistAsync(AuditEntry entry, CancellationToken cancellationToken)
    {
        using var correlationIdLogContext = LogContext.PushProperty("CorrelationId", entry.CorrelationId);
        using var scope = scopeFactory.CreateScope();
        var sinks = scope.ServiceProvider.GetServices<IAuditSink>();

        logger.LogInformation(
            "Persisting audit entry {Id} for {ContainerName}/{EntityId} (operation {Operation}).",
            entry.Id, entry.ContainerName, entry.EntityId, entry.Operation);

        await Task.WhenAll(sinks.Select(sink => sink.PersistAsync(entry, cancellationToken)));
    }
}
