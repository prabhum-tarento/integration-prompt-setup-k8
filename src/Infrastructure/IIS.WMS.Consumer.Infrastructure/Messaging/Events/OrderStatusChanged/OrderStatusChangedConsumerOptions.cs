using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged;

/// <summary>
/// Bound from the <c>Kafka:OrderStatusChanged</c> configuration section - settings for the
/// Avro/Schema-Registry <c>net.pandora.nexus.event.b2b.sales.OrderStatusChanged</c> consumer
/// (docs/events/b2b.sales.OrderStatusChanged.md). <see cref="ConsumerOptions.BootstrapServers"/> and
/// <see cref="ConsumerOptions.SchemaRegistryUrl"/> need not be repeated here for the common case of one
/// Kafka cluster/Schema Registry shared by every consumer - same fallback as
/// <see cref="InternalHallmarkingStatusChanged.InternalHallmarkingStatusChangedConsumerOptions"/>. This
/// event carries no item lines, so there is no <c>MaxItemLineParallelism</c>-equivalent setting here.
/// </summary>
public sealed class OrderStatusChangedConsumerOptions : ConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "Kafka:OrderStatusChanged";
}
