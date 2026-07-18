using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;

/// <summary>Bound from the <c>Audit</c> configuration section.</summary>
public sealed class AuditOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "Audit";

    /// <summary>
    /// Capacity of the bounded in-memory channel <see cref="AuditTrailWriter"/> enqueues onto and
    /// <see cref="AuditBackgroundService"/> drains - large enough to absorb a burst of mutations without
    /// dropping entries (integration-resiliency.instructions.md §6), while still bounding worst-case
    /// memory if the Audit container or Cosmos itself is degraded for a sustained period.
    /// </summary>
    public int ChannelCapacity { get; init; } = 10_000;

    /// <summary>
    /// Whether <see cref="AuditBackgroundService"/> persists each drained entry to the Cosmos
    /// <c>AuditLog</c> container via <see cref="CosmosAuditSink"/>. Defaults to <see langword="true"/>,
    /// preserving this pipeline's original (and only) behavior. Independent of
    /// <see cref="ColdStorageEnabled"/> - when both are <see langword="true"/>, every entry is persisted
    /// to both destinations.
    /// </summary>
    public bool CosmosDbEnabled { get; init; } = true;

    /// <summary>
    /// Whether <see cref="AuditBackgroundService"/> also persists each drained entry to the cold-tier
    /// Blob Storage archive via <see cref="ColdBlobAuditSink"/>
    /// (<c>BlobStorageOptions.AuditArchiveContainerName</c>). Opt-in, defaults to
    /// <see langword="false"/>. Independent of <see cref="CosmosDbEnabled"/>.
    /// </summary>
    public bool ColdStorageEnabled { get; init; }

    /// <summary>
    /// Container names excluded from audit capture entirely - <see cref="AuditTrailWriter.Enqueue"/>
    /// drops entries for these before they reach the channel. Matched case-insensitively against
    /// <see cref="AuditEntry.ContainerName"/>. Empty by default (every container is audited).
    /// </summary>
    public IReadOnlyList<string> ExcludedContainers { get; init; } = [];
}
