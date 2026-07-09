namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Shared state a <see cref="ConsumerHostedService{TValue}"/> updates on every poll and a
/// <see cref="ConsumerHealthCheck"/> reads - keeps the health check decoupled from the
/// Confluent.Kafka consumer instance itself. Each consumer gets its own instance (registered as a
/// keyed singleton, see <see cref="MessagingServiceCollectionExtensions"/>) - a stall in one
/// consumer must not be masked by another consumer still polling successfully.
/// </summary>
public sealed class ConsumerHealthState
{
    /// <summary>UTC timestamp of the most recent successful poll cycle (whether or not it returned a message).</summary>
    public DateTimeOffset LastSuccessfulPollUtc { get; set; } = DateTimeOffset.UtcNow;
}
