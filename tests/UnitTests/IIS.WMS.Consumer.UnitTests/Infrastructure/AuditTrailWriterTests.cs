using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
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
        var writer = CreateWriter(channel);
        var entry = CreateEntry("1");

        writer.Enqueue(entry);

        Assert.True(channel.Reader.TryRead(out var read));
        Assert.Equal(entry.Id, read!.Id);
    }

    [Fact(DisplayName = "Enqueue never throws and never blocks when the channel is full - it drops the entry instead")]
    public void Enqueue_ChannelFull_DoesNotThrowAndDropsEntry()
    {
        var channel = Channel.CreateBounded<AuditEntry>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var writer = CreateWriter(channel);

        writer.Enqueue(CreateEntry("1"));
        writer.Enqueue(CreateEntry("2")); // channel capacity is 1 - this one is dropped, not blocked or thrown

        Assert.True(channel.Reader.TryRead(out var first));
        Assert.Equal("1:guid", first!.Id);
        Assert.False(channel.Reader.TryRead(out _));
    }

    [Fact(DisplayName = "Enqueue drops the entry without writing to the channel when its container is excluded")]
    public void Enqueue_ContainerExcluded_DoesNotWriteToChannel()
    {
        var channel = Channel.CreateBounded<AuditEntry>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var writer = CreateWriter(channel, excludedContainers: ["InventoryEvents"]);

        writer.Enqueue(CreateEntry("1"));

        Assert.False(channel.Reader.TryRead(out _));
    }

    [Fact(DisplayName = "Enqueue matches excluded container names case-insensitively")]
    public void Enqueue_ContainerExcludedDifferentCase_DoesNotWriteToChannel()
    {
        var channel = Channel.CreateBounded<AuditEntry>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var writer = CreateWriter(channel, excludedContainers: ["inventoryevents"]);

        writer.Enqueue(CreateEntry("1"));

        Assert.False(channel.Reader.TryRead(out _));
    }

    [Fact(DisplayName = "Enqueue still writes entries for containers not in the excluded list")]
    public void Enqueue_ContainerNotExcluded_StillWritesToChannel()
    {
        var channel = Channel.CreateBounded<AuditEntry>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var writer = CreateWriter(channel, excludedContainers: ["SomeOtherContainer"]);
        var entry = CreateEntry("1");

        writer.Enqueue(entry);

        Assert.True(channel.Reader.TryRead(out var read));
        Assert.Equal(entry.Id, read!.Id);
    }

    private static AuditTrailWriter CreateWriter(Channel<AuditEntry> channel, IReadOnlyList<string>? excludedContainers = null) =>
        new(channel,
            Options.Create(new AuditOptions { ExcludedContainers = excludedContainers ?? [] }),
            Substitute.For<ILogger<AuditTrailWriter>>());

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
