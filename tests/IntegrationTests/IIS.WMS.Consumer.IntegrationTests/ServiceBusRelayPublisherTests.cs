using System.Text.Json;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Polly.Registry;

namespace IIS.WMS.Consumer.IntegrationTests;

/// <summary>
/// Tests for <see cref="ServiceBusRelayPublisher"/> against <see cref="VirtualServiceBusClient"/>
/// (integration-resiliency.instructions.md §9) and <see cref="InMemoryFileStore"/> - the claim-check
/// threshold, blob offload, per-queue sender reuse, and bulk-batch chunking/grouping this class is
/// responsible for once every caller (Kafka relay consumers, Service Bus-side handlers/services)
/// delegates to it instead of duplicating this logic itself.
/// </summary>
public sealed class ServiceBusRelayPublisherTests
{
    private const string QueueName = "inventory-events";
    private const string LargePayloadContainerName = "large-payload";

    private static (ServiceBusRelayPublisher Publisher, VirtualServiceBusClient Client, InMemoryFileStore FileStore) CreatePublisher()
    {
        var client = new VirtualServiceBusClient();
        var fileStore = new InMemoryFileStore();
        var blobStorageOptions = Options.Create(new BlobStorageOptions { LargePayloadContainerName = LargePayloadContainerName });

        var services = new ServiceCollection();
        services.AddResiliencePipelines();
        var pipelineProvider = services.BuildServiceProvider().GetRequiredService<ResiliencePipelineProvider<string>>();

        var publisher = new ServiceBusRelayPublisher(client, pipelineProvider, fileStore, blobStorageOptions, NullLogger<ServiceBusRelayPublisher>.Instance);
        return (publisher, client, fileStore);
    }

    private static ServiceBusRelayMessage CreateMessage(string json, string messageId, int? maxMessageSizeBytesOverride = null) =>
        new(
            QueueName: QueueName,
            SessionId: "WH1:SKU1",
            MessageId: messageId,
            CorrelationId: $"corr-{messageId}",
            AppId: "test-app",
            Types: ["InventoryStateChanged", "TestConsumer"],
            SourceName: "TestConsumer",
            PayloadName: "InventoryStateChanged",
            Json: json,
            MaxMessageSizeBytesOverride: maxMessageSizeBytesOverride);

    [Fact(DisplayName = "PublishAsync sends a payload under the claim-check threshold inline")]
    public async Task PublishAsync_PayloadUnderThreshold_TravelsInline()
    {
        var (publisher, client, _) = CreatePublisher();
        var message = CreateMessage("""{"sku":"SKU1"}""", "msg-1");

        var result = await publisher.PublishAsync(message);

        Assert.False(result.WasOffloaded);
        Assert.Equal(string.Empty, result.BlobPath);

        var dispatched = Assert.Single(client.Broker.Dispatched);
        Assert.Equal(QueueName, dispatched.QueueName);
        Assert.Equal("msg-1", dispatched.Message.MessageId);
        Assert.Equal("WH1:SKU1", dispatched.Message.SessionId);
        Assert.Equal("corr-msg-1", dispatched.Message.ApplicationProperties["CorrelationId"]);

        var envelope = JsonSerializer.Deserialize<ServiceBusRelayEnvelope>(dispatched.Message.Body.ToString())!;
        Assert.Equal(string.Empty, envelope.BlobPath);
        Assert.Equal("SKU1", envelope.ReflexSchema.GetProperty("sku").GetString());
    }

    [Fact(DisplayName = "PublishAsync offloads a payload over the claim-check threshold to blob storage")]
    public async Task PublishAsync_PayloadOverThreshold_OffloadsToBlob()
    {
        var (publisher, client, fileStore) = CreatePublisher();
        var message = CreateMessage("""{"sku":"SKU1","note":"a payload that is deliberately larger than the tiny threshold below"}""", "msg-2", maxMessageSizeBytesOverride: 16);

        var result = await publisher.PublishAsync(message);

        Assert.True(result.WasOffloaded);
        Assert.NotEqual(string.Empty, result.BlobPath);
        Assert.Single(await fileStore.ListAsync(LargePayloadContainerName));

        var dispatched = Assert.Single(client.Broker.Dispatched);
        var envelope = JsonSerializer.Deserialize<ServiceBusRelayEnvelope>(dispatched.Message.Body.ToString())!;
        Assert.Equal(result.BlobPath, envelope.BlobPath);
        Assert.Equal(JsonValueKind.Object, envelope.ReflexSchema.ValueKind);
        Assert.False(envelope.ReflexSchema.EnumerateObject().Any());
    }

    [Fact(DisplayName = "PublishAsync reuses one cached sender per queue across multiple publishes")]
    public async Task PublishAsync_SameQueueTwice_ReusesSameSender()
    {
        var (publisher, client, _) = CreatePublisher();

        await publisher.PublishAsync(CreateMessage("""{"sku":"SKU1"}""", "msg-3"));
        await publisher.PublishAsync(CreateMessage("""{"sku":"SKU2"}""", "msg-4"));

        Assert.Single(client.CreatedSenders);
        Assert.Contains(QueueName, publisher.CachedServiceBusSenderQueueNames);
    }

    [Fact(DisplayName = "PublishBatchAsync groups messages by queue and relays each to its own queue")]
    public async Task PublishBatchAsync_MessagesAcrossQueues_GroupsPerQueue()
    {
        const string otherQueue = "inventory-bulk-import";
        var (publisher, client, _) = CreatePublisher();

        var messages = new[]
        {
            CreateMessage("""{"sku":"SKU1"}""", "msg-a") with { QueueName = QueueName },
            CreateMessage("""{"sku":"SKU2"}""", "msg-b") with { QueueName = otherQueue },
            CreateMessage("""{"sku":"SKU3"}""", "msg-c") with { QueueName = QueueName },
        };

        var results = await publisher.PublishBatchAsync(messages);

        Assert.Equal(3, results.Count);
        Assert.All(results, r => Assert.False(r.WasOffloaded));

        Assert.Equal(2, client.Broker.Dispatched.Count(d => d.QueueName == QueueName));
        Assert.Single(client.Broker.Dispatched, d => d.QueueName == otherQueue);
        Assert.Equal(2, client.CreatedSenders.Count);
    }

    [Fact(DisplayName = "PublishBatchAsync splits more messages than one batch holds into multiple batches")]
    public async Task PublishBatchAsync_MoreMessagesThanBatchCapacity_SplitsIntoMultipleBatches()
    {
        var (publisher, client, _) = CreatePublisher();

        // Forces the sender to exist before tuning its batch cap - ServiceBusRelayPublisher creates it
        // lazily on first use, and VirtualServiceBusSender.MaxMessagesPerBatch only exists once created.
        await publisher.PublishAsync(CreateMessage("""{"sku":"warmup"}""", "msg-warmup"));
        client.CreatedSenders[QueueName].MaxMessagesPerBatch = 2;

        var messages = Enumerable.Range(1, 5)
            .Select(i => CreateMessage($$"""{"sku":"SKU{{i}}"}""", $"msg-{i}"))
            .ToArray();

        var results = await publisher.PublishBatchAsync(messages);

        Assert.Equal(5, results.Count);
        Assert.Equal([2, 2, 1], client.CreatedSenders[QueueName].SentBatchSizes);
        Assert.Equal(6, client.Broker.Dispatched.Count(d => d.QueueName == QueueName)); // 1 warmup + 5 batched
    }
}
