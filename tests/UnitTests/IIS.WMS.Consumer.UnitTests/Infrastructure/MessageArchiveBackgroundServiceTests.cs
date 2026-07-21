using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="MessageArchiveBackgroundService"/> - draining the MessageArchive
/// channel and fanning each entry out to every registered <see cref="IMessageArchiveSink"/>, and the
/// graceful-shutdown drain (integration-resiliency.instructions.md §6) that keeps a rolling
/// deployment/pod restart from losing whatever is still buffered. Per-sink failure handling is covered
/// by that sink's own tests, not here - this class is deliberately ignorant of what a sink's destination
/// is, including the case where zero sinks are registered at all (both
/// <see cref="MessageArchiveOptions.CosmosDbEnabled"/>/<see cref="MessageArchiveOptions.BlobEnabled"/>
/// false) - a scenario <c>AuditBackgroundServiceTests</c> has no equivalent for, since Audit's
/// registration throws before ever reaching this class in that case.
/// </summary>
public class MessageArchiveBackgroundServiceTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "StopAsync drains every entry already buffered in the channel before returning")]
    public async Task StopAsync_EntriesBuffered_PersistsAllThroughEverySinkBeforeReturning()
    {
        // Signalled from the first sink call so the test can await the drain loop actually starting
        // before completing the writer and calling StopAsync - see AuditBackgroundServiceTests' identical
        // remark on the Task.Run/StartAsync race this avoids.
        var firstEntryStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sink = Substitute.For<IMessageArchiveSink>();
        sink.PersistAsync(Arg.Any<MessageArchive>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
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

        await service.StopAsync(CancellationToken.None);

        foreach (var entry in entries)
        {
            await sink.Received(1).PersistAsync(Arg.Is<MessageArchive>(e => e.Id == entry.Id), Arg.Any<CancellationToken>());
        }
    }

    [Fact(DisplayName = "Every registered IMessageArchiveSink persists the same entry")]
    public async Task StopAsync_MultipleSinksRegistered_PersistsToEverySink()
    {
        var firstEntryStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cosmosSink = Substitute.For<IMessageArchiveSink>();
        cosmosSink.PersistAsync(Arg.Any<MessageArchive>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                firstEntryStarted.TrySetResult();
                return Task.CompletedTask;
            });
        var blobSink = Substitute.For<IMessageArchiveSink>();
        blobSink.PersistAsync(Arg.Any<MessageArchive>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        var (service, channel) = CreateService(cosmosSink, blobSink);
        await service.StartAsync(CancellationToken.None);

        var entry = CreateEntry("1");
        channel.Writer.TryWrite(entry);

        await firstEntryStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        await cosmosSink.Received(1).PersistAsync(Arg.Is<MessageArchive>(e => e.Id == entry.Id), Arg.Any<CancellationToken>());
        await blobSink.Received(1).PersistAsync(Arg.Is<MessageArchive>(e => e.Id == entry.Id), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "StopAsync completes without throwing when zero IMessageArchiveSink are registered")]
    public async Task StopAsync_NoSinksRegistered_CompletesWithoutThrowing()
    {
        var (service, channel) = CreateService();
        await service.StartAsync(CancellationToken.None);

        channel.Writer.TryWrite(CreateEntry("1"));

        // No sink call to await here - Task.WhenAll over zero sinks is already complete, so there is no
        // "first call started" signal to wait on. Polling for the channel to drain is more deterministic
        // than a flat delay - it only waits as long as actually needed for ExecuteAsync's loop to read
        // the buffered entry off the channel before StopAsync completes the writer.
        var deadline = DateTime.UtcNow.Add(TimeSpan.FromSeconds(5));
        while (channel.Reader.Count > 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(10));
        }

        var exception = await Record.ExceptionAsync(() => service.StopAsync(CancellationToken.None));

        Assert.Null(exception);
    }

    private static (MessageArchiveBackgroundService Service, Channel<MessageArchive> Channel) CreateService(params IMessageArchiveSink[] sinks)
    {
        var services = new ServiceCollection();
        foreach (var sink in sinks)
        {
            services.AddScoped(_ => sink);
        }

        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
        var channel = Channel.CreateBounded<MessageArchive>(new BoundedChannelOptions(100) { FullMode = BoundedChannelFullMode.Wait });

        var service = new MessageArchiveBackgroundService(channel, scopeFactory, Substitute.For<ILogger<MessageArchiveBackgroundService>>());

        return (service, channel);
    }

    private static MessageArchive CreateEntry(string suffix) => MessageArchive.Create(
        id: $"InventoryStateChanged_corr-{suffix}",
        category: "InventoryStateChanged",
        payload: "{}",
        correlationId: $"corr-{suffix}",
        timestamp: Now);
}
