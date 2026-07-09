namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Bound from the <c>Kafka:InventoryStateChanged</c> configuration section - settings for the
/// Avro/Schema-Registry <c>net.pandora.nexus.event.inventory.InventoryStateChanged</c> consumer
/// (docs/kafka-event-schema/inventory-state-changes.md). <see cref="ConsumerOptions.BootstrapServers"/>
/// is set here even though it repeats the JSON consumer's <c>Kafka:BootstrapServers</c> value for
/// the same cluster - <see cref="ConsumerOptions"/>'s properties are <see langword="init"/>, so
/// there's no post-binding hook to default one option's value from another's without relaxing that
/// immutability; two lines of appsettings duplication was judged the smaller cost.
/// </summary>
public sealed class InventoryStateChangedConsumerOptions : ConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "Kafka:InventoryStateChanged";

    /// <summary>Confluent Schema Registry URL used to resolve the Avro schema.</summary>
    public string SchemaRegistryUrl { get; init; } = default!;
}
