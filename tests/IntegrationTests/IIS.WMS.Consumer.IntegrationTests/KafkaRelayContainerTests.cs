using Confluent.Kafka;
using Testcontainers.Kafka;

namespace IIS.WMS.Consumer.IntegrationTests;

/// <summary>
/// Testcontainers-based tests against a real (containerized) Kafka broker
/// (integration-resiliency.instructions.md §9) - requires Docker. This class covers only the
/// Kafka leg of the pipeline as a template for the full sweep the doc asks for; a Cosmos DB
/// Emulator container and an Azure Service Bus emulator container are not wired up here (no
/// officially maintained Testcontainers module for either was available at the time this
/// skeleton was generated - the doc's own §9 anticipates this with "where available"). Extending
/// this class to cover the full Kafka → Service Bus → Cosmos DB path, including the
/// redelivery/dedupe case and a forced Cosmos 412, is follow-up work for whoever picks up the
/// emulator container images.
/// </summary>
public sealed class KafkaRelayContainerTests : IAsyncLifetime
{
    private readonly KafkaContainer kafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:7.5.12").Build();

    public Task InitializeAsync() => kafkaContainer.StartAsync();

    public Task DisposeAsync() => kafkaContainer.DisposeAsync().AsTask();

    [Fact(DisplayName = "A message produced to the inventory-events topic is consumed back from the same broker")]
    public async Task ProduceThenConsume_MessageOnInventoryEventsTopic_IsReadBackByConsumer()
    {
        const string topic = "inventory-events";
        var bootstrapServers = kafkaContainer.GetBootstrapAddress();

        using var producer = new ProducerBuilder<string, string>(
            new ProducerConfig { BootstrapServers = bootstrapServers }).Build();
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "WH1:SKU1", Value = """{"eventId":"evt-1","warehouseId":"WH1","sku":"SKU1","quantity":5,"eventType":"Create"}""",
        });

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "inventory-events-consumer-test",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();
        consumer.Subscribe(topic);

        var result = consumer.Consume(TimeSpan.FromSeconds(30));

        Assert.NotNull(result);
        Assert.Equal("WH1:SKU1", result.Message.Key);
    }
}
