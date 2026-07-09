namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Common settings every Kafka → Service Bus relay consumer needs, regardless of wire format
/// (JSON, Avro, or a future schema). A concrete options type per consumer derives from this and
/// adds only what's specific to it (e.g. a Schema Registry URL) - see
/// <see cref="KafkaConsumerOptions"/> and <see cref="InventoryStateChangedConsumerOptions"/>.
/// </summary>
public class ConsumerOptions
{
    /// <summary>
    /// Whether this consumer runs at all. Defaults to <see langword="true"/> so omitting the key
    /// preserves today's always-on behavior; set to <see langword="false"/> per environment to
    /// turn an individual consumer off without removing its configuration section.
    /// </summary>
    public bool Enabled { get; init; } = true;

    /// <summary>Kafka bootstrap servers connection string.</summary>
    public string BootstrapServers { get; init; } = default!;

    /// <summary>Topic this consumer subscribes to.</summary>
    public string Topic { get; init; } = default!;

    /// <summary>Kafka consumer group id.</summary>
    public string ConsumerGroup { get; init; } = default!;

    /// <summary>Service Bus queue this consumer relays onto - the durability boundary (integration-resiliency.instructions.md §1).</summary>
    public string ServiceBusQueueName { get; init; } = default!;

    /// <summary>How long <c>IConsumer.Consume</c> blocks waiting for a message before returning null.</summary>
    public TimeSpan PollTimeout { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Number of concurrent workers draining the bounded channel between the poll loop and the
    /// Service Bus publish step (integration-resiliency.instructions.md §6). Size this to the
    /// target throughput and the downstream publish latency, not to CPU core count - the work is
    /// I/O-bound. Defaults to 1 (no concurrency beyond the poll loop itself), which reproduces the
    /// original strictly-sequential behavior for a low-volume topic; raise it for a high-throughput
    /// topic instead of leaving it at the default and reaching for more partitions/pods first.
    /// </summary>
    public int WorkerCount { get; init; } = 1;

    /// <summary>
    /// Capacity of the bounded channel between the poll loop and the workers above
    /// (integration-resiliency.instructions.md §6). Bounded, not unbounded, so a slow downstream
    /// (Service Bus, not Kafka) applies backpressure to the poll loop instead of buffering an
    /// unbounded, ever-growing backlog in process memory.
    /// </summary>
    public int ChannelCapacity { get; init; } = 1_000;
}
