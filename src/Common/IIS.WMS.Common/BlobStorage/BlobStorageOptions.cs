namespace IIS.WMS.Common.BlobStorage;

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
    /// Hot-tier container <c>Persistence.CosmosDb.Audit.AuditBackgroundService</c> falls back to when
    /// persisting an audit record to the Cosmos <c>AuditLog</c> container fails even after the SDK's
    /// own retry policy (cosmos-db.instructions.md §2) is exhausted - kept separate from
    /// <see cref="ConsumerDeadLetterContainerName"/> since it is a distinct failure domain (the audit
    /// pipeline, not Kafka message relay). Configurable per environment. Lives in the
    /// <see cref="Hot"/> storage account, since it needs prompt investigation like the other dead-letter
    /// container, not archival cold storage.
    /// </summary>
    public string AuditDeadLetterContainerName { get; init; } = "audit-dead-letter";

    /// <summary>
    /// Cold-tier container <c>Persistence.CosmosDb.Audit.ColdBlobAuditSink</c> archives every
    /// <c>AuditEntry</c> to when <c>Audit:ColdStorageEnabled</c> is on - durable archival storage,
    /// distinct from <see cref="RequestAuditContainerName"/> (the Kafka consumer's raw message audit)
    /// and <see cref="AuditDeadLetterContainerName"/> (the hot-tier Cosmos-write-failure fallback).
    /// Configurable per environment. Lives in the <see cref="Cold"/> storage account.
    /// </summary>
    public string AuditArchiveContainerName { get; init; } = "audit-archive";

    /// <summary>
    /// Hot-tier container holding the event validation templates - one <c>{SchemaName}/{EventType}.cs</c>
    /// C# script blob per schema/event type, managed through the event-validation-templates API and
    /// compiled/executed by the Kafka consumer's dynamic-validation step right after each schema
    /// handler's own <c>ValidateAsync</c>. Configurable per environment. Lives in the <see cref="Hot"/>
    /// storage account - templates are read on the consume hot path.
    /// </summary>
    public string ValidationTemplateContainerName { get; init; } = "validation-templates";

    /// <summary>
    /// Hot-tier container for the Kafka consumer's Service Bus claim-check offload
    /// (<c>ConsumerOptions.MaxServiceBusMessageSizeBytes</c>, integration-resiliency.instructions.md
    /// §1/§5) - a schema payload over that threshold is uploaded here instead of being carried inline
    /// in the Service Bus message body, with only the resulting blob path set on
    /// <see cref="Messaging.ServiceBusRelayEnvelope.BlobPath"/>. Configurable per environment; lives in
    /// the <see cref="Hot"/> storage account, same tier as <see cref="ValidationTemplateContainerName"/>.
    /// <b>Producer side only</b>: <c>ServiceBusConsumerHostedService</c> does not yet rehydrate
    /// <see cref="Messaging.ServiceBusRelayEnvelope.BlobPath"/> back into a payload on the consume
    /// side - see that class's own remarks.
    /// </summary>
    public string LargePayloadContainerName { get; init; } = "large-payload";

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
