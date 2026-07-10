namespace IIS.WMS.Consumer.Infrastructure.BlobStorage;

/// <summary>Bound from the <c>BlobStorage</c> configuration section (integration-resiliency.instructions.md §5).</summary>
public sealed class BlobStorageOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "BlobStorage";

    /// <summary>
    /// Cold-tier container the Kafka consumer's dedup-check flow writes every consumed message's raw
    /// header/body audit log to (integration-resiliency.instructions.md §1/§5) - unconditional, not
    /// gated by <see cref="RequestAuditEnabled"/>, which covers a separate, still-optional
    /// request/response audit use.
    /// </summary>
    public const string RequestAuditContainerName = "request-audit";

    /// <summary>
    /// Hot-tier container the Kafka consumer writes a message's raw header/body to when it fails
    /// Avro/JSON deserialization (integration-resiliency.instructions.md §1) - kept separate from the
    /// <c>imports</c>/<c>exports</c> containers documented in §5, which are reserved for actual
    /// import/export files, not failure audit records.
    /// </summary>
    public const string ConsumerDeadLetterContainerName = "consumer-dead-letter";

    /// <summary>Blob Storage account URI.</summary>
    public string AccountUri { get; init; } = default!;

    /// <summary>Feature flag for the optional cold-tier request/response audit log - off unless explicitly enabled per environment.</summary>
    public bool RequestAuditEnabled { get; init; }
}
