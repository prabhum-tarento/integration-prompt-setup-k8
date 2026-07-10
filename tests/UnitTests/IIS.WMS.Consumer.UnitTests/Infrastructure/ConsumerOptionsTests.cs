using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="ConsumerOptions.ApplyKafkaLevelDefaults"/> - the event-level-
/// first, Kafka-level-fallback merge for <c>Enabled</c>, <c>BootstrapServers</c>, and
/// <c>SchemaRegistryUrl</c>.
/// </summary>
public class ConsumerOptionsTests
{
    [Fact(DisplayName = "Unset event-level settings fall back to the Kafka-level values")]
    public void ApplyKafkaLevelDefaults_EventLevelUnset_FallsBackToKafkaLevel()
    {
        var kafkaLevel = new KafkaConsumerOptions
        {
            Enabled = true,
            BootstrapServers = "kafka-level:9092",
            SchemaRegistryUrl = "http://kafka-level-schema-registry",
        };
        var eventLevel = new InventoryStateChangedConsumerOptions();

        eventLevel.ApplyKafkaLevelDefaults(kafkaLevel);

        Assert.True(eventLevel.Enabled);
        Assert.Equal("kafka-level:9092", eventLevel.BootstrapServers);
        Assert.Equal("http://kafka-level-schema-registry", eventLevel.SchemaRegistryUrl);
    }

    [Fact(DisplayName = "Configured event-level settings win over the Kafka-level values")]
    public void ApplyKafkaLevelDefaults_EventLevelConfigured_EventLevelWins()
    {
        var kafkaLevel = new KafkaConsumerOptions
        {
            Enabled = true,
            BootstrapServers = "kafka-level:9092",
            SchemaRegistryUrl = "http://kafka-level-schema-registry",
        };
        var eventLevel = new InventoryStateChangedConsumerOptions
        {
            Enabled = false,
            BootstrapServers = "event-level:9092",
            SchemaRegistryUrl = "http://event-level-schema-registry",
        };

        eventLevel.ApplyKafkaLevelDefaults(kafkaLevel);

        Assert.False(eventLevel.Enabled);
        Assert.Equal("event-level:9092", eventLevel.BootstrapServers);
        Assert.Equal("http://event-level-schema-registry", eventLevel.SchemaRegistryUrl);
    }

    [Fact(DisplayName = "A partially configured event level only falls back for the settings it left unset")]
    public void ApplyKafkaLevelDefaults_EventLevelPartiallyConfigured_MergesPerSetting()
    {
        var kafkaLevel = new KafkaConsumerOptions
        {
            Enabled = true,
            BootstrapServers = "kafka-level:9092",
            SchemaRegistryUrl = "http://kafka-level-schema-registry",
        };
        var eventLevel = new InventoryStateChangedConsumerOptions
        {
            BootstrapServers = "event-level:9092",
        };

        eventLevel.ApplyKafkaLevelDefaults(kafkaLevel);

        Assert.True(eventLevel.Enabled);
        Assert.Equal("event-level:9092", eventLevel.BootstrapServers);
        Assert.Equal("http://kafka-level-schema-registry", eventLevel.SchemaRegistryUrl);
    }
}
