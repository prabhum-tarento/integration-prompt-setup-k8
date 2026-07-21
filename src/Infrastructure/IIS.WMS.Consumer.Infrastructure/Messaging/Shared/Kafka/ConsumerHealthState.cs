namespace IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;

/// <summary>
/// Shared state a <see cref="KafkaConsumerHostedServiceBase"/> updates on every poll and a
/// <see cref="ConsumerHealthCheck"/> reads - keeps the health check decoupled from the
/// Confluent.Kafka consumer instance itself. Each event type a consumer registers via
/// <see cref="KafkaConsumerHostedServiceBase.RegisterSchemaHandlers"/> gets its own instance (built internally
/// by the consumer, not injected or DI-registered - see <see cref="KafkaConsumerHostedServiceBase.GetHealthState"/>
/// and <c>MessagingServiceCollectionExtensions.AddKafkaConsumer</c>) - a stall on one event type must
/// not be masked by another event type (or another consumer) still polling successfully.
/// </summary>
public sealed class ConsumerHealthState
{
    /// <summary>UTC timestamp of the most recent successful poll cycle (whether or not it returned a message).</summary>
    public DateTimeOffset LastSuccessfulPollUtc { get; set; } = DateTimeOffset.UtcNow;
}
