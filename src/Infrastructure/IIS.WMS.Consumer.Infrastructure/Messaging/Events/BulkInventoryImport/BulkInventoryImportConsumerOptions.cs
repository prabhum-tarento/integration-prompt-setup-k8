using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.BulkInventoryImport;

/// <summary>
/// Bound from the <c>Kafka:BulkInventoryImport</c> configuration section - settings for the
/// high-volume (~millions of messages per burst), unordered bulk-import consumer
/// (integration-resiliency.instructions.md §1). <see cref="ConsumerOptions.BootstrapServers"/> and
/// <see cref="ConsumerOptions.SchemaRegistryUrl"/> fall back to the top-level <c>Kafka</c> section
/// when left unset, same as <see cref="InventoryStateChangedConsumerOptions"/> - set them here only
/// if this topic genuinely lives on a different cluster/registry. Unlike the other two consumers,
/// <see cref="ConsumerOptions.WorkerCount"/>/<see cref="ConsumerOptions.ChannelCapacity"/> should be
/// tuned materially higher here, and the Kafka topic itself needs more partitions than the other two
/// topics for KEDA to actually scale consumer pods out across a burst - see this section's own
/// appsettings comment.
/// </summary>
public sealed class BulkInventoryImportConsumerOptions : ConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "Kafka:BulkInventoryImport";
}
