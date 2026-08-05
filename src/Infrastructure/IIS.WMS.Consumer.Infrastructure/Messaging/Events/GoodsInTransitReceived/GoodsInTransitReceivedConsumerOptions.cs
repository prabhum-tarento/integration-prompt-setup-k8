using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived;

/// <summary>
/// Bound from the <c>Kafka:GoodsInTransitReceived</c> configuration section - settings for the
/// Avro/Schema-Registry <c>net.pandora.nexus.event.b2b.purchase.GoodsInTransitReceived</c> consumer
/// (docs/events/b2b.purchase.GoodsInTransitReceived.md). <see cref="ConsumerOptions.BootstrapServers"/> and
/// <see cref="ConsumerOptions.SchemaRegistryUrl"/> need not be repeated here for the common case of one
/// Kafka cluster/Schema Registry shared by every consumer - same fallback as
/// <see cref="OrderStatusChanged.OrderStatusChangedConsumerOptions"/>.
/// </summary>
public sealed class GoodsInTransitReceivedConsumerOptions : ConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "Kafka:GoodsInTransitReceived";
}
