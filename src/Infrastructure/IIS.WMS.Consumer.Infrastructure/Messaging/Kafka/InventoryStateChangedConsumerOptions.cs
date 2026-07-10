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
}
