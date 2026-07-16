using IIS.WMS.Consumer.Infrastructure.Messaging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>Tests for <see cref="ServiceBusSenderCacheService"/>, with <see cref="IServiceBusSenderCacheSource"/> mocked so no real Kafka consumer needs to be constructed.</summary>
public class ServiceBusSenderCacheServiceTests
{
    [Fact(DisplayName = "ListCachedSenders returns one entry per registered source")]
    public void ListCachedSenders_MultipleSources_ReturnsOneEntryPerSource()
    {
        var kafkaSource = Substitute.For<IServiceBusSenderCacheSource>();
        kafkaSource.ConsumerName.Returns("KafkaConsumerHostedService");
        kafkaSource.CachedServiceBusSenderQueueNames.Returns(["inventory-events"]);

        var bulkImportSource = Substitute.For<IServiceBusSenderCacheSource>();
        bulkImportSource.ConsumerName.Returns("BulkInventoryImportConsumerHostedService");
        bulkImportSource.CachedServiceBusSenderQueueNames.Returns(["inventory-bulk-import"]);

        var sut = new ServiceBusSenderCacheService([kafkaSource, bulkImportSource]);

        var entries = sut.ListCachedSenders();

        Assert.Equal(2, entries.Count);
        Assert.Contains(entries, e => e.ConsumerName == "KafkaConsumerHostedService" && e.QueueNames.SequenceEqual(["inventory-events"]));
        Assert.Contains(entries, e => e.ConsumerName == "BulkInventoryImportConsumerHostedService" && e.QueueNames.SequenceEqual(["inventory-bulk-import"]));
    }

    [Fact(DisplayName = "ClearCachedSendersAsync clears every registered source")]
    public async Task ClearCachedSendersAsync_MultipleSources_ClearsEverySource()
    {
        var firstSource = Substitute.For<IServiceBusSenderCacheSource>();
        var secondSource = Substitute.For<IServiceBusSenderCacheSource>();
        var sut = new ServiceBusSenderCacheService([firstSource, secondSource]);

        await sut.ClearCachedSendersAsync(CancellationToken.None);

        await firstSource.Received(1).ClearServiceBusSendersAsync();
        await secondSource.Received(1).ClearServiceBusSendersAsync();
    }

    [Fact(DisplayName = "ClearCachedSendersAsync stops before clearing further sources when cancelled")]
    public async Task ClearCachedSendersAsync_CancelledBeforeCall_ThrowsAndSkipsRemainingSources()
    {
        var firstSource = Substitute.For<IServiceBusSenderCacheSource>();
        var secondSource = Substitute.For<IServiceBusSenderCacheSource>();
        var sut = new ServiceBusSenderCacheService([firstSource, secondSource]);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => sut.ClearCachedSendersAsync(cts.Token));

        await firstSource.DidNotReceive().ClearServiceBusSendersAsync();
        await secondSource.DidNotReceive().ClearServiceBusSendersAsync();
    }
}
