using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="AuditTrailWriter"/> - the non-blocking channel hand-off every
/// <c>CosmosRepository{TDomain,TDocument}</c> mutation enqueues onto.
/// </summary>
public class AuditTrailWriterTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "Enqueue writes the entry onto the channel when capacity is available")]
    public void Enqueue_ChannelHasCapacity_EntryIsReadable()
    {
        var channel = Channel.CreateBounded<AuditEntry>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var writer = new AuditTrailWriter(channel, Substitute.For<ILogger<AuditTrailWriter>>());
        var entry = CreateEntry("1");

        writer.Enqueue(entry);

        Assert.True(channel.Reader.TryRead(out var read));
        Assert.Equal(entry.Id, read!.Id);
    }

    [Fact(DisplayName = "Enqueue never throws and never blocks when the channel is full - it drops the entry instead")]
    public void Enqueue_ChannelFull_DoesNotThrowAndDropsEntry()
    {
        var channel = Channel.CreateBounded<AuditEntry>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var writer = new AuditTrailWriter(channel, Substitute.For<ILogger<AuditTrailWriter>>());

        writer.Enqueue(CreateEntry("1"));
        writer.Enqueue(CreateEntry("2")); // channel capacity is 1 - this one is dropped, not blocked or thrown

        Assert.True(channel.Reader.TryRead(out var first));
        Assert.Equal("1:guid", first!.Id);
        Assert.False(channel.Reader.TryRead(out _));
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
