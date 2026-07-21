using Confluent.Kafka;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;

/// <summary>
/// Common settings every Kafka → Service Bus relay consumer needs, regardless of wire format
/// (JSON, Avro, or a future schema). A concrete options type per consumer derives from this and
/// adds only what's specific to it - see <see cref="KafkaConsumerOptions"/> and
/// <see cref="InventoryStateChangedConsumerOptions"/>.
/// </summary>
/// <remarks>
/// <see cref="Enabled"/>, <see cref="BootstrapServers"/>, <see cref="SchemaRegistryUrl"/>,
/// <see cref="SchemaRegistryApiKey"/>, <see cref="SchemaRegistryApiSecret"/>,
/// <see cref="WorkerCount"/>, <see cref="ChannelCapacity"/>, <see cref="DeduplicationCheckEnabled"/>,
/// <see cref="IgnoreCorrelationIdPrefixes"/>, <see cref="IgnoreCorrelationIdSuffixes"/>,
/// <see cref="Protocol"/>, <see cref="AuthenticationMode"/>, <see cref="Username"/>,
/// <see cref="Password"/>, <see cref="EnableAutoCommit"/>, and <see cref="AutoOffsetReset"/> are all
/// resolved event-level-first, Kafka-level-fallback: an event-level consumer (e.g.
/// <c>Kafka:InventoryStateChanged</c>) that leaves one of these unset inherits it from the
/// Kafka-level (top-level <c>Kafka</c> section) value instead of having to repeat it, via
/// <see cref="ApplyKafkaLevelDefaults"/> (wired up in
/// <see cref="MessagingServiceCollectionExtensions.AddMessaging"/>). That's why these are mutable
/// and nullable rather than <see langword="init"/>-only like the rest of this type - the fallback
/// has to fill them in after configuration binding runs, before any consumer reads them. The
/// top-level <c>Kafka</c> section itself bottoms out at hardcoded defaults for all of them (see
/// <see cref="MessagingServiceCollectionExtensions.AddMessaging"/>'s <c>PostConfigure</c>), so by
/// the time any consumer runs, every one of these is guaranteed non-null - callers still read them
/// defensively (e.g. <c>?? 1</c>) rather than asserting that, since a options POCO built directly in
/// a unit test (bypassing the DI <c>PostConfigure</c> pipeline) wouldn't get that guarantee.
/// </remarks>
public class ConsumerOptions
{
    /// <summary>
    /// Hardcoded fallback for <see cref="MaxServiceBusMessageSizeBytes"/> when unset at both the event
    /// and Kafka level - 200 KiB, a conservative margin under Service Bus Standard tier's 256 KB
    /// per-message limit that leaves headroom for <see cref="IIS.WMS.Common.Messaging.ServiceBusRelayEnvelope"/>'s own
    /// fields (<c>CorrelationId</c>/<c>AppId</c>/<c>Type</c>/<c>BlobPath</c>) wrapped around the schema
    /// payload.
    /// </summary>
    public const int DefaultMaxServiceBusMessageSizeBytes = 200 * 1024;

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
    /// Transport security protocol for the broker connection (<c>Plaintext</c>, <c>Ssl</c>,
    /// <c>SaslPlaintext</c>, or <c>SaslSsl</c> - see <see cref="Confluent.Kafka.SecurityProtocol"/>).
    /// Unset leaves Confluent.Kafka's own default (<c>Plaintext</c>) in effect - the right choice for
    /// the local emulator, never for a real cluster. Event level falls back to Kafka level if unset -
    /// see remarks.
    /// </summary>
    public SecurityProtocol? Protocol { get; set; }

    /// <summary>
    /// SASL mechanism (<c>Plain</c>, <c>ScramSha256</c>, <c>ScramSha512</c>, <c>OAuthBearer</c>, or
    /// <c>Gssapi</c> - see <see cref="Confluent.Kafka.SaslMechanism"/>), required alongside
    /// <see cref="Username"/>/<see cref="Password"/> whenever <see cref="Protocol"/> is
    /// <c>SaslPlaintext</c> or <c>SaslSsl</c> - Confluent.Kafka itself rejects an inconsistent
    /// combination (e.g. a mechanism set without a SASL protocol) when the consumer is built, so this
    /// type doesn't duplicate that validation. Event level falls back to Kafka level if unset - see
    /// remarks.
    /// </summary>
    public SaslMechanism? AuthenticationMode { get; set; }

    /// <summary>
    /// SASL username. Never set in <c>appsettings.json</c> - local development reads it from
    /// user-secrets, every other environment from Azure Key Vault, per
    /// engineering-standards.instructions.md §6. Event level falls back to Kafka level if unset - see
    /// remarks.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// SASL password - a secret; same source rule as <see cref="Username"/>. Event level falls back
    /// to Kafka level if unset - see remarks.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// Whether Confluent.Kafka auto-commits offsets on its own background timer, instead of purely
    /// through this service's manual, <see cref="PartitionOffsetCommitTracker"/>-driven commit
    /// (integration-resiliency.instructions.md §1). <b>Correctness-sensitive, not just a style
    /// choice</b>: this service's entire "commit only once a message's flow reaches a terminal
    /// outcome" guarantee assumes manual control over when an offset advances. Setting this
    /// <see langword="true"/> for a topic hands offset advancement back to librdkafka's own timer,
    /// which can commit an offset before this service has actually finished processing (or
    /// dead-lettered) the corresponding message - on a crash in that window, the message is silently
    /// skipped on restart rather than redelivered. Event level falls back to Kafka level if unset (see
    /// remarks), which itself bottoms out at <see langword="false"/> (see
    /// <see cref="MessagingServiceCollectionExtensions.AddMessaging"/>'s <c>PostConfigure</c>) -
    /// Confluent.Kafka's own default if left unset entirely is <see langword="true"/>, the opposite of
    /// what this service needs, which is why the Kafka level always pins an explicit value rather than
    /// leaving this to librdkafka.
    /// </summary>
    public bool? EnableAutoCommit { get; set; }

    /// <summary>
    /// Where a new consumer group (or one whose previously committed offset is no longer valid, e.g.
    /// it aged out of the topic's retention) starts reading a partition - <c>Earliest</c> (replay the
    /// full retained history) or <c>Latest</c> (only messages produced from now on). Event level falls
    /// back to Kafka level if unset (see remarks), which itself bottoms out at <c>Earliest</c> (see
    /// <see cref="MessagingServiceCollectionExtensions.AddMessaging"/>'s <c>PostConfigure</c>),
    /// preserving this service's original behavior - librdkafka's own default if left entirely unset
    /// is <c>Latest</c>.
    /// </summary>
    public AutoOffsetReset? AutoOffsetReset { get; set; }

    /// <summary>
    /// Confluent Schema Registry URL used to resolve an Avro writer schema. Only meaningful for
    /// Avro-contract consumers (e.g. <see cref="InventoryStateChangedConsumerOptions"/>); a
    /// JSON-contract consumer such as <see cref="KafkaConsumerOptions"/> never reads its own copy of
    /// this but can still set it to supply the Kafka-level fallback for Avro consumers under it -
    /// see remarks.
    /// </summary>
    public string? SchemaRegistryUrl { get; set; }

    /// <summary>
    /// Schema Registry API key, distinct from <see cref="Username"/> (Confluent Cloud issues a
    /// separate API key/secret pair per Kafka cluster and per Schema Registry - the Kafka
    /// bootstrap-server credentials do not authenticate against the registry). Left unset, the
    /// registry client sends no credentials at all, which is correct for the local emulator but
    /// gets a 401 from a real Confluent Cloud registry that requires them. Same secret-source rule
    /// as <see cref="Username"/>/<see cref="Password"/>. Event level falls back to Kafka level if
    /// unset - see remarks.
    /// </summary>
    public string? SchemaRegistryApiKey { get; set; }

    /// <summary>
    /// Schema Registry API secret, paired with <see cref="SchemaRegistryApiKey"/> - see its remarks.
    /// Same secret-source rule as <see cref="Username"/>/<see cref="Password"/>. Event level falls
    /// back to Kafka level if unset - see remarks.
    /// </summary>
    public string? SchemaRegistryApiSecret { get; set; }

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
    /// I/O-bound. Event level falls back to Kafka level if unset (see remarks), which itself
    /// bottoms out at 1 (no concurrency beyond the poll loop itself) - reproduces the original
    /// strictly-sequential behavior for a low-volume topic; a high-throughput topic should set this
    /// explicitly at its own event level rather than relying on the Kafka-level fallback.
    /// </summary>
    public int? WorkerCount { get; set; }

    /// <summary>
    /// Capacity of the bounded channel between the poll loop and the workers above
    /// (integration-resiliency.instructions.md §6). Bounded, not unbounded, so a slow downstream
    /// (Service Bus, not Kafka) applies backpressure to the poll loop instead of buffering an
    /// unbounded, ever-growing backlog in process memory. Event level falls back to Kafka level if
    /// unset (see remarks), which itself bottoms out at 1,000.
    /// </summary>
    public int? ChannelCapacity { get; set; }

    /// <summary>
    /// Whether this consumer checks each message against
    /// <see cref="Application.Common.IDeduplicationService"/> before validating/publishing it
    /// (integration-resiliency.instructions.md §1). Event level
    /// falls back to Kafka level if unset (see remarks), which itself bottoms out at
    /// <see langword="true"/>, preserving today's always-on behavior. Set to <see langword="false"/>
    /// for a topic where Nexus dedup coverage doesn't apply or isn't worth the extra call - the
    /// downstream Service Bus consumer's own idempotency check (§2) still applies regardless, so
    /// disabling this is a latency/cost trade-off, not a correctness one.
    /// </summary>
    public bool? DeduplicationCheckEnabled { get; set; }

    /// <summary>
    /// Ignore-list of <c>Correlation-Id</c> prefixes (integration-resiliency.instructions.md §1) - a
    /// message whose correlation id starts with any entry here (or matches
    /// <see cref="IgnoreCorrelationIdSuffixes"/>) is skipped entirely: not deserialized, not
    /// audit-logged, not deduplicated, not published - just logged at <c>Information</c> and its
    /// offset committed forward, the same "valid but deliberately not relayed" treatment as
    /// <c>ValidateAsync</c> returning <see langword="false"/>. Event level falls back to Kafka level
    /// if unset (see remarks); unset (or empty) at both levels means no filtering - every message is
    /// accepted, which is also what happens for any individual message that has no
    /// <c>Correlation-Id</c> header at all (nothing to match a prefix/suffix against). Matching is
    /// ordinal and case-insensitive.
    /// </summary>
    public string[]? IgnoreCorrelationIdPrefixes { get; set; }

    /// <summary>Ignore-list of <c>Correlation-Id</c> suffixes - see <see cref="IgnoreCorrelationIdPrefixes"/>, which this pairs with (either matching is enough to ignore the message).</summary>
    public string[]? IgnoreCorrelationIdSuffixes { get; set; }

    /// <summary>
    /// Claim-check threshold, in bytes, for the relayed schema payload (the JSON written to
    /// <see cref="IIS.WMS.Common.Messaging.ServiceBusRelayEnvelope.ReflexSchema"/>) - a payload at or under this size
    /// travels inline in the Service Bus message body as always; one over it is instead uploaded to the
    /// hot-tier <see cref="IIS.WMS.Common.BlobStorage.BlobStorageOptions.LargePayloadContainerName"/> container, with
    /// only its blob path carried in <see cref="IIS.WMS.Common.Messaging.ServiceBusRelayEnvelope.BlobPath"/> (
    /// <c>ReflexSchema</c> left empty in that case). Event level falls back to Kafka level if unset (see
    /// remarks), which itself bottoms out at <see cref="DefaultMaxServiceBusMessageSizeBytes"/>.
    /// </summary>
    public int? MaxServiceBusMessageSizeBytes { get; set; }

    /// <summary>
    /// Fills <see cref="Enabled"/>, <see cref="BootstrapServers"/>, <see cref="SchemaRegistryUrl"/>,
    /// <see cref="SchemaRegistryApiKey"/>, <see cref="SchemaRegistryApiSecret"/>,
    /// <see cref="WorkerCount"/>, <see cref="ChannelCapacity"/>,
    /// <see cref="DeduplicationCheckEnabled"/>, <see cref="IgnoreCorrelationIdPrefixes"/>,
    /// <see cref="IgnoreCorrelationIdSuffixes"/>, <see cref="Protocol"/>,
    /// <see cref="AuthenticationMode"/>, <see cref="Username"/>, <see cref="Password"/>,
    /// <see cref="EnableAutoCommit"/>, <see cref="AutoOffsetReset"/>, and
    /// <see cref="MaxServiceBusMessageSizeBytes"/> from
    /// <paramref name="kafkaLevelOptions"/> wherever this (event-level) instance left them unset -
    /// event level wins whenever it's configured, Kafka level is only the fallback. Called once per
    /// event-level options type from an
    /// <see cref="Microsoft.Extensions.Options.IPostConfigureOptions{TOptions}"/> registration (see
    /// <see cref="MessagingServiceCollectionExtensions.AddMessaging"/>), after both sections have
    /// been bound and after <paramref name="kafkaLevelOptions"/> has already had its own defaults
    /// applied if unset.
    /// </summary>
    /// <param name="kafkaLevelOptions">The resolved top-level <c>Kafka</c> section options this event's unset settings fall back to.</param>
    public void ApplyKafkaLevelDefaults(ConsumerOptions kafkaLevelOptions)
    {
        Enabled ??= kafkaLevelOptions.Enabled;
        BootstrapServers ??= kafkaLevelOptions.BootstrapServers;
        SchemaRegistryUrl ??= kafkaLevelOptions.SchemaRegistryUrl;
        SchemaRegistryApiKey ??= kafkaLevelOptions.SchemaRegistryApiKey;
        SchemaRegistryApiSecret ??= kafkaLevelOptions.SchemaRegistryApiSecret;
        WorkerCount ??= kafkaLevelOptions.WorkerCount;
        ChannelCapacity ??= kafkaLevelOptions.ChannelCapacity;
        DeduplicationCheckEnabled ??= kafkaLevelOptions.DeduplicationCheckEnabled;
        IgnoreCorrelationIdPrefixes ??= kafkaLevelOptions.IgnoreCorrelationIdPrefixes;
        IgnoreCorrelationIdSuffixes ??= kafkaLevelOptions.IgnoreCorrelationIdSuffixes;
        Protocol ??= kafkaLevelOptions.Protocol;
        AuthenticationMode ??= kafkaLevelOptions.AuthenticationMode;
        Username ??= kafkaLevelOptions.Username;
        Password ??= kafkaLevelOptions.Password;
        EnableAutoCommit ??= kafkaLevelOptions.EnableAutoCommit;
        AutoOffsetReset ??= kafkaLevelOptions.AutoOffsetReset;
        MaxServiceBusMessageSizeBytes ??= kafkaLevelOptions.MaxServiceBusMessageSizeBytes;
    }

    /// <summary>
    /// Whether <paramref name="correlationId"/> should be skipped per
    /// <see cref="IgnoreCorrelationIdPrefixes"/>/<see cref="IgnoreCorrelationIdSuffixes"/>. Always
    /// <see langword="false"/> (accept) if <paramref name="correlationId"/> is null/empty, or if
    /// neither list has any entries - filtering is opt-in, not opt-out.
    /// </summary>
    /// <param name="correlationId">The message's raw <c>Correlation-Id</c> header value, or <see langword="null"/> if it had none.</param>
    public bool IsCorrelationIdIgnored(string? correlationId)
    {
        if (string.IsNullOrEmpty(correlationId))
        {
            return false;
        }

        var matchesPrefix = IgnoreCorrelationIdPrefixes is { Length: > 0 } prefixes
            && prefixes.Any(prefix => correlationId.StartsWith(prefix, StringComparison.OrdinalIgnoreCase));

        var matchesSuffix = IgnoreCorrelationIdSuffixes is { Length: > 0 } suffixes
            && suffixes.Any(suffix => correlationId.EndsWith(suffix, StringComparison.OrdinalIgnoreCase));

        return matchesPrefix || matchesSuffix;
    }
}
