namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Bound from the <c>Kafka:InventoryStateChanged</c> configuration section - settings for the
/// Avro/Schema-Registry <c>net.pandora.nexus.event.inventory.InventoryStateChanged</c> consumer
/// (docs/kafka-event-schema/inventory-state-changes.md). <see cref="ConsumerOptions.BootstrapServers"/>
/// and <see cref="ConsumerOptions.SchemaRegistryUrl"/> need not be repeated here for the common case
/// of one Kafka cluster/Schema Registry shared by every consumer - leave them unset and they fall
/// back to the top-level <c>Kafka</c> section's values via
/// <see cref="ConsumerOptions.ApplyKafkaLevelDefaults"/>; set them here only when this consumer
/// genuinely needs to point at a different cluster or registry.
/// </summary>
public sealed class InventoryStateChangedConsumerOptions : ConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "Kafka:InventoryStateChanged";

    /// <summary>
    /// Service Bus queue for <c>InventoryAdjusted</c> events specifically - unset (the default) means
    /// both event types this consumer relays share <see cref="ConsumerOptions.ServiceBusQueueName"/>,
    /// same as before this setting existed. Set this only if <c>InventoryAdjusted</c> needs to land on
    /// a different queue than <c>InventoryStateChanged</c> - see
    /// <see cref="InventoryStateChangedConsumerHostedService"/>.
    /// </summary>
    public string? InventoryAdjustedServiceBusQueueName { get; set; }
}
