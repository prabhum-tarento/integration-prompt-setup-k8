using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.OrderArchiving;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="OrderArchiveWriter"/> - the non-blocking channel hand-off
/// <c>ConsumerHostedService</c> enqueues an <see cref="OrderArchive"/> onto instead of upserting to
/// Cosmos inline.
/// </summary>
public class OrderArchiveWriterTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "Enqueue writes the entry onto the channel when capacity is available")]
    public void Enqueue_ChannelHasCapacity_EntryIsReadable()
    {
        var channel = Channel.CreateBounded<OrderArchive>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var writer = new OrderArchiveWriter(channel, Substitute.For<ILogger<OrderArchiveWriter>>());
        var entry = CreateEntry("1");

        writer.Enqueue(entry);

        Assert.True(channel.Reader.TryRead(out var read));
        Assert.Equal(entry.Id, read!.Id);
    }

    [Fact(DisplayName = "Enqueue never throws and never blocks when the channel is full - it drops the entry instead")]
    public void Enqueue_ChannelFull_DoesNotThrowAndDropsEntry()
    {
        var channel = Channel.CreateBounded<OrderArchive>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var writer = new OrderArchiveWriter(channel, Substitute.For<ILogger<OrderArchiveWriter>>());

        writer.Enqueue(CreateEntry("1"));
        writer.Enqueue(CreateEntry("2")); // channel capacity is 1 - this one is dropped, not blocked or thrown

        Assert.True(channel.Reader.TryRead(out var first));
        Assert.Equal("InventoryStateChanged_corr-1", first!.Id);
        Assert.False(channel.Reader.TryRead(out _));
    }

    private static OrderArchive CreateEntry(string correlationSuffix) => OrderArchive.Create(
        id: $"InventoryStateChanged_corr-{correlationSuffix}",
        category: "WH1:SKU1",
        orderDetailJson: "{}",
        correlationId: $"corr-{correlationSuffix}",
        timestamp: Now);
}
