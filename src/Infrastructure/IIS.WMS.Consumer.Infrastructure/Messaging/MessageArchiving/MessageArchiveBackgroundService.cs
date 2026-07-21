using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog.Context;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;

/// <summary>
/// Drains the bounded <see cref="MessageArchive"/> <see cref="Channel{T}"/> <see cref="MessageArchiveWriter"/>
/// enqueues onto and fans each entry out to every registered <see cref="IMessageArchiveSink"/> - the
/// background half of the non-blocking MessageArchive pipeline (integration-resiliency.instructions.md
/// §6's <c>Channel&lt;T&gt;</c> pattern), mirroring <c>Persistence.CosmosDb.Audit.AuditBackgroundService</c>.
/// Which sink(s) are registered depends on <see cref="MessageArchiveOptions.CosmosDbEnabled"/>/
/// <see cref="MessageArchiveOptions.BlobEnabled"/> (see
/// <c>MessageArchiveServiceCollectionExtensions.AddMessageArchiving</c>) - both may be
/// <see langword="false"/>, in which case <see cref="Task.WhenAll(IEnumerable{Task})"/> over zero sinks
/// below is a no-op and the drained entry is simply dropped (deliberate divergence from the audit
/// pipeline - see <see cref="MessageArchiveOptions.CosmosDbEnabled"/>'s remarks). This class itself has
/// no knowledge of Cosmos or Blob Storage.
/// </summary>
/// <remarks>
/// <b>Durability across a restart/redeploy.</b> <see cref="StopAsync"/> completes the channel's writer
/// instead of relying on <see cref="BackgroundService"/>'s default cancellation-token behavior, and
/// <see cref="ExecuteAsync"/> deliberately reads with <see cref="CancellationToken.None"/> rather than
/// the stopping token - so on a graceful shutdown every entry already buffered in the channel is still
/// drained and persisted before the process exits, not abandoned mid-flight. A hard crash (SIGKILL,
/// OOM-kill, host power loss) runs no shutdown code at all, so whatever is buffered in the channel at
/// that instant is lost - an inherent limitation of any in-memory buffer, the same trade-off already
/// accepted by <c>AuditBackgroundService</c>/<c>OrderArchiveBackgroundService</c>.
/// </remarks>
public sealed class MessageArchiveBackgroundService(
    Channel<MessageArchive> channel,
    IServiceScopeFactory scopeFactory,
    ILogger<MessageArchiveBackgroundService> logger) : BackgroundService
{
    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("MessageArchive background worker starting.");

        // CancellationToken.None, not stoppingToken: see this class's remarks on graceful-shutdown
        // draining. StopAsync below completes the channel writer, which is what ends this enumeration
        // once every buffered entry has been read - not cancellation.
        await foreach (var entry in channel.Reader.ReadAllAsync(CancellationToken.None))
        {
            await PersistAsync(entry, CancellationToken.None);
        }

        logger.LogInformation("MessageArchive background worker drained the channel and is stopping.");
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
    /// Fans one entry out to every registered <see cref="IMessageArchiveSink"/>, through a fresh DI
    /// scope - <see cref="IMessageArchiveSink"/> implementations are Scoped (they depend on Scoped
    /// services like <see cref="Application.Common.IMessageArchiveRepository"/>,
    /// cosmos-db.instructions.md §12), so this singleton <see cref="BackgroundService"/> cannot hold
    /// them directly. Each sink is contracted to never throw (see
    /// <see cref="IMessageArchiveSink.PersistAsync"/>), so one sink's failure never prevents the others
    /// from running; zero registered sinks makes <see cref="Task.WhenAll(IEnumerable{Task})"/> a no-op.
    /// Pushes <see cref="MessageArchive.CorrelationId"/> onto Serilog's ambient <see cref="LogContext"/>
    /// (integration-resiliency.instructions.md §7) so every log line emitted while persisting - including
    /// inside the sinks themselves - carries it.
    /// </summary>
    private async Task PersistAsync(MessageArchive entry, CancellationToken cancellationToken)
    {
        using var correlationIdLogContext = LogContext.PushProperty("CorrelationId", entry.CorrelationId);
        using var scope = scopeFactory.CreateScope();
        var sinks = scope.ServiceProvider.GetServices<IMessageArchiveSink>();

        logger.LogInformation(
            "Persisting message archive entry {Id}/{Category}.", entry.Id, entry.Category);

        await Task.WhenAll(sinks.Select(sink => sink.PersistAsync(entry, cancellationToken)));
    }
}
