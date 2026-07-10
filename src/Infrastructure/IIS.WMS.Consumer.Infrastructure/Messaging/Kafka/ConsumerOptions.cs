namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Common settings every Kafka → Service Bus relay consumer needs, regardless of wire format
/// (JSON, Avro, or a future schema). A concrete options type per consumer derives from this and
/// adds only what's specific to it - see <see cref="KafkaConsumerOptions"/> and
/// <see cref="InventoryStateChangedConsumerOptions"/>.
/// </summary>
/// <remarks>
/// <see cref="Enabled"/>, <see cref="BootstrapServers"/>, and <see cref="SchemaRegistryUrl"/> are
/// resolved event-level-first, Kafka-level-fallback: an event-level consumer (e.g.
/// <c>Kafka:InventoryStateChanged</c>) that leaves one of these three unset inherits it from the
/// Kafka-level (top-level <c>Kafka</c> section) value instead of having to repeat it, via
/// <see cref="ApplyKafkaLevelDefaults"/> (wired up in
/// <see cref="MessagingServiceCollectionExtensions.AddMessaging"/>). That's why these three are
/// mutable and nullable rather than <see langword="init"/>-only like the rest of this type - the
/// fallback has to fill them in after configuration binding runs, before any consumer reads them.
/// </remarks>
public class ConsumerOptions
{
    /// <summary>
    /// Whether this consumer runs at all. Unset (<see langword="null"/>) at the event level falls
    /// back to the Kafka-level value (see remarks); unset at the Kafka level itself defaults to
    /// <see langword="true"/>, preserving today's always-on behavior. Set to
    /// <see langword="false"/> at whichever level to turn a consumer off without removing its
    /// configuration section.
    /// </summary>
    public bool? Enabled { get; set; }

    /// <summary>Kafka bootstrap servers connection string. Event level falls back to Kafka level if unset - see remarks.</summary>
    public string? BootstrapServers { get; set; }

    /// <summary>
    /// Confluent Schema Registry URL used to resolve an Avro writer schema. Only meaningful for
    /// Avro-contract consumers (e.g. <see cref="InventoryStateChangedConsumerOptions"/>); a
    /// JSON-contract consumer such as <see cref="KafkaConsumerOptions"/> never reads its own copy of
    /// this but can still set it to supply the Kafka-level fallback for Avro consumers under it -
    /// see remarks.
    /// </summary>
    public string? SchemaRegistryUrl { get; set; }

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

    /// <summary>
    /// Fills <see cref="Enabled"/>, <see cref="BootstrapServers"/>, and
    /// <see cref="SchemaRegistryUrl"/> from <paramref name="kafkaLevelOptions"/> wherever this
    /// (event-level) instance left them unset - event level wins whenever it's configured, Kafka
    /// level is only the fallback. Called once per event-level options type from an
    /// <see cref="Microsoft.Extensions.Options.IPostConfigureOptions{TOptions}"/> registration (see
    /// <see cref="MessagingServiceCollectionExtensions.AddMessaging"/>), after both sections have
    /// been bound and after <paramref name="kafkaLevelOptions"/> has already had its own
    /// <see cref="Enabled"/> defaulted to <see langword="true"/> if unset.
    /// </summary>
    /// <param name="kafkaLevelOptions">The resolved top-level <c>Kafka</c> section options this event's unset settings fall back to.</param>
    public void ApplyKafkaLevelDefaults(ConsumerOptions kafkaLevelOptions)
    {
        Enabled ??= kafkaLevelOptions.Enabled;
        BootstrapServers ??= kafkaLevelOptions.BootstrapServers;
        SchemaRegistryUrl ??= kafkaLevelOptions.SchemaRegistryUrl;
    }
}
