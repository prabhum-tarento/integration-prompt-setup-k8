using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="MessageArchiveWriter"/> - the non-blocking channel hand-off used to
/// enqueue a <see cref="MessageArchive"/> instead of persisting to Cosmos/Blob inline.
/// </summary>
public class MessageArchiveWriterTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "Enqueue writes the entry onto the channel when capacity is available")]
    public void Enqueue_ChannelHasCapacity_EntryIsReadable()
    {
        var channel = Channel.CreateBounded<MessageArchive>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var writer = new MessageArchiveWriter(channel, Substitute.For<ILogger<MessageArchiveWriter>>());
        var entry = CreateEntry("1");

        writer.Enqueue(entry);

        Assert.True(channel.Reader.TryRead(out var read));
        Assert.Equal(entry.Id, read!.Id);
    }

    [Fact(DisplayName = "Enqueue never throws and never blocks when the channel is full - it drops the entry instead")]
    public void Enqueue_ChannelFull_DoesNotThrowAndDropsEntry()
    {
        var channel = Channel.CreateBounded<MessageArchive>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var writer = new MessageArchiveWriter(channel, Substitute.For<ILogger<MessageArchiveWriter>>());

        writer.Enqueue(CreateEntry("1"));
        writer.Enqueue(CreateEntry("2")); // channel capacity is 1 - this one is dropped, not blocked or thrown

        Assert.True(channel.Reader.TryRead(out var first));
        Assert.Equal("InventoryStateChanged_corr-1", first!.Id);
        Assert.False(channel.Reader.TryRead(out _));
    }

    private static MessageArchive CreateEntry(string correlationSuffix) => MessageArchive.Create(
        id: $"InventoryStateChanged_corr-{correlationSuffix}",
        category: "InventoryStateChanged",
        payload: "{}",
        correlationId: $"corr-{correlationSuffix}",
        timestamp: Now);
}
