using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged;

/// <summary>
/// Bound from the <c>Kafka:InternalHallmarkingStatusChanged</c> configuration section - settings for
/// the Avro/Schema-Registry <c>net.pandora.nexus.event.inventory.InternalHallmarkingStatusChanged</c>
/// consumer (docs/events/inventory.InternalHallmarkingStatusChanged.md). <see cref="ConsumerOptions.BootstrapServers"/>
/// and <see cref="ConsumerOptions.SchemaRegistryUrl"/> need not be repeated here for the common case
/// of one Kafka cluster/Schema Registry shared by every consumer - see
/// <see cref="InventoryEvents.InventoryEventConsumerOptions"/>'s own remarks on the same fallback.
/// Unlike <see cref="InventoryEvents.InventoryEventConsumerOptions"/>, this event carries a single
/// item line, so there is no <c>MaxItemLineParallelism</c>-equivalent setting here.
/// </summary>
public sealed class InternalHallmarkingStatusChangedConsumerOptions : ConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "Kafka:InternalHallmarkingStatusChanged";
}
