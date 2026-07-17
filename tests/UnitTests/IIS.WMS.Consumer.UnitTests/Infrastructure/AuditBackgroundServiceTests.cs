using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="AuditBackgroundService"/> - draining the audit channel and fanning
/// each entry out to every registered <see cref="IAuditSink"/>, and the graceful-shutdown drain
/// (integration-resiliency.instructions.md §6) that keeps a rolling deployment/pod restart from losing
/// whatever is still buffered. Per-sink failure handling (e.g. the Cosmos sink's hot-tier dead-letter
/// fallback) is covered by that sink's own tests, not here - this class is deliberately ignorant of what
/// a sink's destination is.
/// </summary>
public class AuditBackgroundServiceTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "StopAsync drains every entry already buffered in the channel before returning")]
    public async Task StopAsync_EntriesBuffered_PersistsAllThroughEverySinkBeforeReturning()
    {
        // Signalled from the first sink call so the test can await the drain loop actually starting
        // before completing the writer and calling StopAsync - BackgroundService.StartAsync schedules
        // ExecuteAsync via Task.Run(action, token) against its own internal CancellationTokenSource, and
        // if that token is cancelled before the ThreadPool dequeues the work item, Task.Run skips
        // invoking the delegate entirely (transitioning straight to Canceled) rather than running it.
        // That race never matters in production - StartAsync and StopAsync are always seconds/minutes
        // apart there - but calling them back-to-back here would otherwise make this test flaky under
        // parallel test-run ThreadPool contention.
        var firstEntryStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sink = Substitute.For<IAuditSink>();
        sink.PersistAsync(Arg.Any<AuditEntry>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                firstEntryStarted.TrySetResult();
                return Task.CompletedTask;
            });

        var (service, channel) = CreateService(sink);
        await service.StartAsync(CancellationToken.None);

        var entries = Enumerable.Range(1, 5).Select(i => CreateEntry(i.ToString())).ToList();
        foreach (var entry in entries)
        {
            Assert.True(channel.Writer.TryWrite(entry));
        }

        await firstEntryStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Completes the writer and awaits the drain loop to its natural end - see
        // AuditBackgroundService.StopAsync's remarks on why this bounds the drain instead of
        // abandoning buffered entries.
        await service.StopAsync(CancellationToken.None);

        foreach (var entry in entries)
        {
            await sink.Received(1).PersistAsync(Arg.Is<AuditEntry>(e => e.Id == entry.Id), Arg.Any<CancellationToken>());
        }
    }

    [Fact(DisplayName = "Every registered IAuditSink persists the same entry")]
    public async Task StopAsync_MultipleSinksRegistered_PersistsToEverySink()
    {
        var firstEntryStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cosmosSink = Substitute.For<IAuditSink>();
        cosmosSink.PersistAsync(Arg.Any<AuditEntry>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                firstEntryStarted.TrySetResult();
                return Task.CompletedTask;
            });
        var coldSink = Substitute.For<IAuditSink>();
        coldSink.PersistAsync(Arg.Any<AuditEntry>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        var (service, channel) = CreateService(cosmosSink, coldSink);
        await service.StartAsync(CancellationToken.None);

        var entry = CreateEntry("1");
        channel.Writer.TryWrite(entry);

        await firstEntryStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        await cosmosSink.Received(1).PersistAsync(Arg.Is<AuditEntry>(e => e.Id == entry.Id), Arg.Any<CancellationToken>());
        await coldSink.Received(1).PersistAsync(Arg.Is<AuditEntry>(e => e.Id == entry.Id), Arg.Any<CancellationToken>());
    }

    private static (AuditBackgroundService Service, Channel<AuditEntry> Channel) CreateService(params IAuditSink[] sinks)
    {
        var services = new ServiceCollection();
        foreach (var sink in sinks)
        {
            services.AddScoped(_ => sink);
        }

        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
        var channel = Channel.CreateBounded<AuditEntry>(new BoundedChannelOptions(100) { FullMode = BoundedChannelFullMode.Wait });

        var service = new AuditBackgroundService(channel, scopeFactory, Substitute.For<ILogger<AuditBackgroundService>>());

        return (service, channel);
    }

    private static AuditEntry CreateEntry(string suffix) => AuditEntry.Create(
        id: $"{suffix}:guid",
        containerName: "InventoryEvents",
        entityId: $"WH1:SKU{suffix}",
        entityPartitionKey: $"WH1:SKU{suffix}",
        operation: AuditOperation.Create,
        correlationId: "corr-1",
        schema: "InventoryStateChanged",
        documentJson: "{}",
        timestampUtc: Now);
}
