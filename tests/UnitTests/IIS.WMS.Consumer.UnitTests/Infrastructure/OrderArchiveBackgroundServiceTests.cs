using System.Threading.Channels;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.OrderArchiving;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="OrderArchiveBackgroundService"/> - draining the OrderArchive channel
/// into <see cref="IOrderArchiveRepository"/>, and the graceful-shutdown drain
/// (integration-resiliency.instructions.md §6) that keeps a rolling deployment/pod restart from losing
/// whatever is still buffered. Unlike <c>AuditBackgroundService</c>, a Cosmos write failure here is
/// logged Critical and the entry dropped - no blob dead-letter fallback (see this service's own remarks).
/// </summary>
public class OrderArchiveBackgroundServiceTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "StopAsync drains every entry already buffered in the channel before returning")]
    public async Task StopAsync_EntriesBuffered_PersistsAllThroughRepositoryBeforeReturning()
    {
        // See AuditBackgroundServiceTests' equivalent test for why this waits for the drain loop to
        // actually start before completing the writer and calling StopAsync back-to-back with
        // StartAsync.
        var firstEntryStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var repository = Substitute.For<IOrderArchiveRepository>();
        repository.UpsertAsync(Arg.Any<OrderArchive>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                firstEntryStarted.TrySetResult();
                return Task.FromResult(callInfo.Arg<OrderArchive>());
            });

        var (service, channel) = CreateService(repository);
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
            await repository.Received(1).UpsertAsync(Arg.Is<OrderArchive>(e => e.Id == entry.Id), Arg.Any<CancellationToken>());
        }
    }

    [Fact(DisplayName = "A Cosmos write failure is logged and the entry dropped - it never crashes the drain loop")]
    public async Task StopAsync_RepositoryThrows_DrainLoopContinuesAndDropsEntry()
    {
        var entryStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var repository = Substitute.For<IOrderArchiveRepository>();
        repository.UpsertAsync(Arg.Any<OrderArchive>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                entryStarted.TrySetResult();
                return Task.FromException<OrderArchive>(new InvalidOperationException("Cosmos is unavailable."));
            });

        var (service, channel) = CreateService(repository);
        await service.StartAsync(CancellationToken.None);

        var entry = CreateEntry("1");
        channel.Writer.TryWrite(entry);

        await entryStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // The drain loop must still complete cleanly - StopAsync returning at all (rather than hanging
        // or throwing) proves the failed persist didn't crash ExecuteAsync's foreach.
        await service.StopAsync(CancellationToken.None);

        await repository.Received(1).UpsertAsync(Arg.Is<OrderArchive>(e => e.Id == entry.Id), Arg.Any<CancellationToken>());
    }

    private static (OrderArchiveBackgroundService Service, Channel<OrderArchive> Channel) CreateService(
        IOrderArchiveRepository repository)
    {
        var services = new ServiceCollection();
        services.AddScoped(_ => repository);
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

        var channel = Channel.CreateBounded<OrderArchive>(new BoundedChannelOptions(100) { FullMode = BoundedChannelFullMode.Wait });

        var service = new OrderArchiveBackgroundService(
            channel, scopeFactory, Substitute.For<ILogger<OrderArchiveBackgroundService>>());

        return (service, channel);
    }

    private static OrderArchive CreateEntry(string suffix) => OrderArchive.Create(
        id: $"InventoryStateChanged_corr-{suffix}",
        category: $"WH1:SKU{suffix}",
        orderDetailJson: "{}",
        correlationId: $"corr-{suffix}",
        timestamp: Now);
}
