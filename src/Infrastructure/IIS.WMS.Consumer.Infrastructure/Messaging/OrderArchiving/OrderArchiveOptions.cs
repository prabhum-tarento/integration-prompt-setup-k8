namespace IIS.WMS.Consumer.Infrastructure.Messaging.OrderArchiving;

/// <summary>Bound from the <c>OrderArchive</c> configuration section.</summary>
public sealed class OrderArchiveOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "OrderArchive";

    /// <summary>
    /// Capacity of the bounded in-memory channel <see cref="OrderArchiveWriter"/> enqueues onto and
    /// <see cref="OrderArchiveBackgroundService"/> drains - large enough to absorb a burst of messages
    /// without dropping entries (integration-resiliency.instructions.md §6), while still bounding
    /// worst-case memory if the OrderArchive container or Cosmos itself is degraded for a sustained
    /// period.
    /// </summary>
    public int ChannelCapacity { get; init; } = 10_000;
}
