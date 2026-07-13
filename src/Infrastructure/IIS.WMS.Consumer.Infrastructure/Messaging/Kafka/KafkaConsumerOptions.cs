namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Bound from the <c>Kafka</c> configuration section - settings for the JSON-contract inventory
/// events consumer, and also the Kafka-level fallback/allow-list parent for every event-level
/// consumer under it (<see cref="ConsumerOptions.ApplyKafkaLevelDefaults"/>,
/// <see cref="KafkaEventFunctions"/>).
/// </summary>
public sealed class KafkaConsumerOptions : ConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "Kafka";

    /// <summary>
    /// Allow-list of Kafka consumer names to start - mirrors an Azure Functions host's
    /// <c>functions</c> filter (run only the named functions), not a per-consumer disable. Names
    /// match <see cref="KafkaEvents.InventoryEventsConsumerKey"/>, <see cref="KafkaEvents.InventoryStateChangedEventType"/>
    /// (the Avro consumer's registration key, even though it also relays <see cref="KafkaEvents.InventoryAdjustedEventType"/>),
    /// or <see cref="KafkaEvents.BulkInventoryImportConsumerKey"/>. This
    /// gates whether a consumer's hosted service and health check are registered at all
    /// (<see cref="MessagingServiceCollectionExtensions.AddMessaging"/>) - it sits above, and is
    /// independent of, each consumer's own <see cref="ConsumerOptions.Enabled"/> flag, which instead
    /// makes a registered consumer's <c>ExecuteAsync</c> a no-op. <see langword="null"/> or empty
    /// means "no filter - every consumer whose own <c>Enabled</c> resolves to <see langword="true"/>
    /// starts."
    /// </summary>
    public string[]? KafkaEventFunctions { get; init; }
}
