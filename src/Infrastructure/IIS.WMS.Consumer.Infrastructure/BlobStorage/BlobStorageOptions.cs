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
    /// request/response audit use. Configurable per environment; defaults to the container name this
    /// was hardcoded to before this became a setting. Lives in the <see cref="Cold"/> storage account.
    /// </summary>
    public string RequestAuditContainerName { get; init; } = "request-audit";

    /// <summary>
    /// Hot-tier container the Kafka consumer writes a message's raw header/body to when it fails
    /// Avro/JSON deserialization (integration-resiliency.instructions.md §1) - kept separate from the
    /// <c>imports</c>/<c>exports</c> containers documented in §5, which are reserved for actual
    /// import/export files, not failure audit records. Configurable per environment; defaults to the
    /// container name this was hardcoded to before this became a setting. Lives in the <see cref="Hot"/>
    /// storage account.
    /// </summary>
    public string ConsumerDeadLetterContainerName { get; init; } = "consumer-dead-letter";

    /// <summary>
    /// Hot-tier Blob Storage account - <c>imports</c>/<c>exports</c> and <see cref="ConsumerDeadLetterContainerName"/>
    /// live here. A distinct account from <see cref="Cold"/>, not just a distinct container, since hot
    /// and cold data have different access-frequency and retention needs and this deployment's two
    /// tiers are backed by separate Storage accounts with their own connection info.
    /// </summary>
    public BlobStorageAccountOptions Hot { get; init; } = new();

    /// <summary>
    /// Cold-tier Blob Storage account - <see cref="RequestAuditContainerName"/> and the general
    /// request/response audit log (<see cref="RequestAuditEnabled"/>) live here. A distinct account
    /// from <see cref="Hot"/> - see <see cref="Hot"/> for why.
    /// </summary>
    public BlobStorageAccountOptions Cold { get; init; } = new();

    /// <summary>Feature flag for the optional cold-tier request/response audit log - off unless explicitly enabled per environment.</summary>
    public bool RequestAuditEnabled { get; init; }
}

/// <summary>Connection settings for one tier's Blob Storage account (integration-resiliency.instructions.md §5).</summary>
public sealed class BlobStorageAccountOptions
{
    /// <summary>
    /// Blob Storage account URI. Used with <see cref="Azure.Identity.DefaultAzureCredential"/>
    /// (AKS Workload Identity) outside local development - the local-dev connection string is read
    /// directly off configuration instead (<c>BlobStorage:Hot:ConnectionString</c> /
    /// <c>BlobStorage:Cold:ConnectionString</c>, via user-secrets), not bound onto this options type.
    /// </summary>
    public string AccountUri { get; init; } = default!;
}
