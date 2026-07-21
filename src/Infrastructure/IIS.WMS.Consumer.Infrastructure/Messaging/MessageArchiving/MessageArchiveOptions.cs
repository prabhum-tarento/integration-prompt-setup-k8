namespace IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;

/// <summary>Bound from the <c>MessageArchive</c> configuration section.</summary>
public sealed class MessageArchiveOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "MessageArchive";

    /// <summary>
    /// Capacity of the bounded in-memory channel <see cref="MessageArchiveWriter"/> enqueues onto and
    /// <see cref="MessageArchiveBackgroundService"/> drains - large enough to absorb a burst of messages
    /// without dropping entries (integration-resiliency.instructions.md §6), while still bounding
    /// worst-case memory if the MessageArchive destination(s) are degraded for a sustained period.
    /// </summary>
    public int ChannelCapacity { get; init; } = 10_000;

    /// <summary>
    /// Whether <see cref="MessageArchiveBackgroundService"/> persists each drained entry to the Cosmos
    /// <c>MessageArchive</c> container via <see cref="CosmosMessageArchiveSink"/>. Defaults to
    /// <see langword="true"/>. Independent of <see cref="BlobEnabled"/> - when both are
    /// <see langword="false"/>, no <see cref="IMessageArchiveSink"/> is registered at all and every
    /// drained entry is simply dropped (deliberate divergence from <c>AuditOptions</c>, which throws at
    /// startup in that case - a message archive is a purely optional diagnostic aid, not a feature that
    /// must always persist somewhere).
    /// </summary>
    public bool CosmosDbEnabled { get; init; } = true;

    /// <summary>
    /// Whether <see cref="MessageArchiveBackgroundService"/> also persists each drained entry to the
    /// cold-tier Blob Storage archive via <see cref="BlobMessageArchiveSink"/>
    /// (<c>BlobStorageOptions.MessageArchiveContainerName</c>). Opt-in, defaults to
    /// <see langword="false"/>. Independent of <see cref="CosmosDbEnabled"/>.
    /// </summary>
    public bool BlobEnabled { get; init; }
}
