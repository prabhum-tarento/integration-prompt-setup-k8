using Confluent.Kafka;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryEvents;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;

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

    [Fact(DisplayName = "No configured prefixes or suffixes accepts every correlation id")]
    public void IsCorrelationIdIgnored_NoConfiguredPatterns_ReturnsFalse()
    {
        var options = new KafkaConsumerOptions();

        Assert.False(options.IsCorrelationIdIgnored("TEST-abc123"));
    }

    [Fact(DisplayName = "A null or empty correlation id is always accepted, even with patterns configured")]
    public void IsCorrelationIdIgnored_NullOrEmptyCorrelationId_ReturnsFalse()
    {
        var options = new KafkaConsumerOptions
        {
            IgnoreCorrelationIdPrefixes = ["TEST-"],
            IgnoreCorrelationIdSuffixes = ["-loadtest"],
        };

        Assert.False(options.IsCorrelationIdIgnored(null));
        Assert.False(options.IsCorrelationIdIgnored(string.Empty));
    }

    [Fact(DisplayName = "A correlation id matching a configured prefix is ignored")]
    public void IsCorrelationIdIgnored_MatchesConfiguredPrefix_ReturnsTrue()
    {
        var options = new KafkaConsumerOptions
        {
            IgnoreCorrelationIdPrefixes = ["TEST-"],
        };

        Assert.True(options.IsCorrelationIdIgnored("TEST-abc123"));
    }

    [Fact(DisplayName = "Prefix matching is case-insensitive")]
    public void IsCorrelationIdIgnored_PrefixDifferentCase_ReturnsTrue()
    {
        var options = new KafkaConsumerOptions
        {
            IgnoreCorrelationIdPrefixes = ["test-"],
        };

        Assert.True(options.IsCorrelationIdIgnored("TEST-abc123"));
    }

    [Fact(DisplayName = "A correlation id matching a configured suffix is ignored")]
    public void IsCorrelationIdIgnored_MatchesConfiguredSuffix_ReturnsTrue()
    {
        var options = new KafkaConsumerOptions
        {
            IgnoreCorrelationIdSuffixes = ["-loadtest"],
        };

        Assert.True(options.IsCorrelationIdIgnored("abc123-loadtest"));
    }

    [Fact(DisplayName = "A correlation id matching neither a configured prefix nor suffix is accepted")]
    public void IsCorrelationIdIgnored_MatchesNeither_ReturnsFalse()
    {
        var options = new KafkaConsumerOptions
        {
            IgnoreCorrelationIdPrefixes = ["TEST-"],
            IgnoreCorrelationIdSuffixes = ["-loadtest"],
        };

        Assert.False(options.IsCorrelationIdIgnored("abc123"));
    }

    [Fact(DisplayName = "Unset event-level ignore patterns fall back to the Kafka-level values")]
    public void ApplyKafkaLevelDefaults_IgnorePatternsUnset_FallsBackToKafkaLevel()
    {
        var kafkaLevel = new KafkaConsumerOptions
        {
            IgnoreCorrelationIdPrefixes = ["TEST-"],
            IgnoreCorrelationIdSuffixes = ["-loadtest"],
        };
        var eventLevel = new InventoryStateChangedConsumerOptions();

        eventLevel.ApplyKafkaLevelDefaults(kafkaLevel);

        Assert.NotNull(eventLevel.IgnoreCorrelationIdPrefixes);
        Assert.NotNull(eventLevel.IgnoreCorrelationIdSuffixes);
        Assert.Equal(["TEST-"], eventLevel.IgnoreCorrelationIdPrefixes);
        Assert.Equal(["-loadtest"], eventLevel.IgnoreCorrelationIdSuffixes);
    }

    [Fact(DisplayName = "Unset event-level broker security settings fall back to the Kafka-level values")]
    public void ApplyKafkaLevelDefaults_SecuritySettingsUnset_FallsBackToKafkaLevel()
    {
        var kafkaLevel = new KafkaConsumerOptions
        {
            Protocol = SecurityProtocol.SaslSsl,
            AuthenticationMode = SaslMechanism.ScramSha512,
            Username = "kafka-level-user",
            Password = "kafka-level-password",
        };
        var eventLevel = new InventoryStateChangedConsumerOptions();

        eventLevel.ApplyKafkaLevelDefaults(kafkaLevel);

        Assert.Equal(SecurityProtocol.SaslSsl, eventLevel.Protocol);
        Assert.Equal(SaslMechanism.ScramSha512, eventLevel.AuthenticationMode);
        Assert.Equal("kafka-level-user", eventLevel.Username);
        Assert.Equal("kafka-level-password", eventLevel.Password);
    }

    [Fact(DisplayName = "Configured event-level broker security settings win over the Kafka-level values")]
    public void ApplyKafkaLevelDefaults_SecuritySettingsConfigured_EventLevelWins()
    {
        var kafkaLevel = new KafkaConsumerOptions
        {
            Protocol = SecurityProtocol.SaslSsl,
            AuthenticationMode = SaslMechanism.ScramSha512,
            Username = "kafka-level-user",
            Password = "kafka-level-password",
        };
        var eventLevel = new InventoryStateChangedConsumerOptions
        {
            Protocol = SecurityProtocol.Ssl,
            AuthenticationMode = SaslMechanism.Plain,
            Username = "event-level-user",
            Password = "event-level-password",
        };

        eventLevel.ApplyKafkaLevelDefaults(kafkaLevel);

        Assert.Equal(SecurityProtocol.Ssl, eventLevel.Protocol);
        Assert.Equal(SaslMechanism.Plain, eventLevel.AuthenticationMode);
        Assert.Equal("event-level-user", eventLevel.Username);
        Assert.Equal("event-level-password", eventLevel.Password);
    }

    [Fact(DisplayName = "Unset event-level Schema Registry credentials fall back to the Kafka-level values")]
    public void ApplyKafkaLevelDefaults_SchemaRegistryCredentialsUnset_FallsBackToKafkaLevel()
    {
        var kafkaLevel = new KafkaConsumerOptions
        {
            SchemaRegistryApiKey = "kafka-level-key",
            SchemaRegistryApiSecret = "kafka-level-secret",
        };
        var eventLevel = new InventoryStateChangedConsumerOptions();

        eventLevel.ApplyKafkaLevelDefaults(kafkaLevel);

        Assert.Equal("kafka-level-key", eventLevel.SchemaRegistryApiKey);
        Assert.Equal("kafka-level-secret", eventLevel.SchemaRegistryApiSecret);
    }

    [Fact(DisplayName = "Configured event-level Schema Registry credentials win over the Kafka-level values")]
    public void ApplyKafkaLevelDefaults_SchemaRegistryCredentialsConfigured_EventLevelWins()
    {
        var kafkaLevel = new KafkaConsumerOptions
        {
            SchemaRegistryApiKey = "kafka-level-key",
            SchemaRegistryApiSecret = "kafka-level-secret",
        };
        var eventLevel = new InventoryStateChangedConsumerOptions
        {
            SchemaRegistryApiKey = "event-level-key",
            SchemaRegistryApiSecret = "event-level-secret",
        };

        eventLevel.ApplyKafkaLevelDefaults(kafkaLevel);

        Assert.Equal("event-level-key", eventLevel.SchemaRegistryApiKey);
        Assert.Equal("event-level-secret", eventLevel.SchemaRegistryApiSecret);
    }

    [Fact(DisplayName = "Unset event-level EnableAutoCommit/AutoOffsetReset fall back to the Kafka-level values")]
    public void ApplyKafkaLevelDefaults_CommitSettingsUnset_FallsBackToKafkaLevel()
    {
        var kafkaLevel = new KafkaConsumerOptions
        {
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        var eventLevel = new InventoryStateChangedConsumerOptions();

        eventLevel.ApplyKafkaLevelDefaults(kafkaLevel);

        Assert.False(eventLevel.EnableAutoCommit);
        Assert.Equal(AutoOffsetReset.Earliest, eventLevel.AutoOffsetReset);
    }

    [Fact(DisplayName = "Configured event-level AutoOffsetReset wins over the Kafka-level value")]
    public void ApplyKafkaLevelDefaults_AutoOffsetResetConfigured_EventLevelWins()
    {
        var kafkaLevel = new KafkaConsumerOptions
        {
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        var eventLevel = new InventoryStateChangedConsumerOptions
        {
            AutoOffsetReset = AutoOffsetReset.Latest,
        };

        eventLevel.ApplyKafkaLevelDefaults(kafkaLevel);

        Assert.Equal(AutoOffsetReset.Latest, eventLevel.AutoOffsetReset);
    }
}
