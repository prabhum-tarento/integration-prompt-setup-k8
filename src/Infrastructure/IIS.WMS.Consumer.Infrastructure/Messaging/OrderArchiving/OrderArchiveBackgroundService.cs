using System.Threading.Channels;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog.Context;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.OrderArchiving;

/// <summary>
/// Drains the bounded <see cref="OrderArchive"/> <see cref="Channel{T}"/> <see cref="OrderArchiveWriter"/>
/// enqueues onto and persists each entry through <see cref="IOrderArchiveRepository"/> - the background
/// half of the non-blocking OrderArchive pipeline (integration-resiliency.instructions.md §6's
/// <c>Channel&lt;T&gt;</c> pattern), mirroring <c>AuditBackgroundService</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Durability across a restart/redeploy.</b> <see cref="StopAsync"/> completes the channel's writer
/// instead of relying on <see cref="BackgroundService"/>'s default cancellation-token behavior, and
/// <see cref="ExecuteAsync"/> deliberately reads with <see cref="CancellationToken.None"/> rather than
/// the stopping token - so on a graceful shutdown every entry already buffered in the channel is still
/// drained and persisted before the process exits, not abandoned mid-flight. Same trade-off as
/// <c>AuditBackgroundService</c> for a hard crash (SIGKILL/OOM-kill/power loss): whatever is buffered in
/// the channel at that instant is lost, an inherent limitation of any in-memory buffer.
/// </para>
/// <para>
/// <b>No blob dead-letter fallback (deliberate deviation from <c>AuditBackgroundService</c>).</b> An
/// <see cref="OrderArchive"/> is a write-once side-channel audit record with no downstream consumer
/// depending on it (<see cref="IOrderArchiveRepository.UpsertAsync"/> is unconditional, no ETag), and
/// Cosmos throttling is already retried by the SDK's own <c>MaxRetryAttemptsOnRateLimitedRequests</c>
/// policy - so a Cosmos write failure here is logged Critical and the entry is dropped, rather than
/// re-serializing enough context to write a comparable hot-tier dead-letter blob. Flagged for reviewer:
/// if parity with the audit pipeline's dead-letter fallback is wanted, add it here.
/// </para>
/// </remarks>
public sealed class OrderArchiveBackgroundService(
    Channel<OrderArchive> channel,
    IServiceScopeFactory scopeFactory,
    ILogger<OrderArchiveBackgroundService> logger) : BackgroundService
{
    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("OrderArchive background worker starting.");

        // CancellationToken.None, not stoppingToken: see this class's remarks on graceful-shutdown
        // draining. StopAsync below completes the channel writer, which is what ends this enumeration
        // once every buffered entry has been read - not cancellation.
        await foreach (var entry in channel.Reader.ReadAllAsync(CancellationToken.None))
        {
            await PersistAsync(entry, CancellationToken.None);
        }

        logger.LogInformation("OrderArchive background worker drained the channel and is stopping.");
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
    /// Persists one entry through a fresh DI scope - <see cref="IOrderArchiveRepository"/> is Scoped
    /// (cosmos-db.instructions.md §12, same as every other repository), so this singleton
    /// <see cref="BackgroundService"/> cannot hold one directly. A failure is logged Critical and the
    /// entry is dropped - see this class's remarks for why there is no dead-letter fallback here.
    /// Pushes <see cref="OrderArchive.CorrelationId"/> onto Serilog's ambient <see cref="LogContext"/>
    /// (integration-resiliency.instructions.md §7) so every log line for this persist - success or
    /// failure - carries it, the same convention <c>KafkaConsumerHostedServiceBase</c> uses at the Kafka boundary.
    /// </summary>
    private async Task PersistAsync(OrderArchive entry, CancellationToken cancellationToken)
    {
        using var correlationIdLogContext = LogContext.PushProperty("CorrelationId", entry.CorrelationId);

        try
        {
            using var scope = scopeFactory.CreateScope();
            var orderArchiveRepository = scope.ServiceProvider.GetRequiredService<IOrderArchiveRepository>();

            await orderArchiveRepository.UpsertAsync(entry, cancellationToken);

            logger.LogInformation(
                "Persisted OrderArchive entry {Id}/{Category} to Cosmos.", entry.Id, entry.Category);
        }
        catch (Exception ex)
        {
            logger.LogCritical(
                ex,
                "Failed to persist OrderArchive entry {Id}/{Category} to Cosmos - this archive record is lost.",
                entry.Id, entry.Category);
        }
    }
}
