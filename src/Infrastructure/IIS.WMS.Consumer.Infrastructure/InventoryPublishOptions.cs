namespace IIS.WMS.Consumer.Infrastructure;

/// <summary>
/// Bound from the <c>InventoryPublish</c> configuration section - the Service Bus queue names the
/// §3.6/3.7/3.8 publishers relay onto (docs/InventoryStateChangedFullQueueTrigger.md). Reflex's
/// equivalent sends are commented out and never define real queue names; this service publishes for
/// real, so these are required, not optional, configuration.
/// </summary>
public sealed class InventoryPublishOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "InventoryPublish";

    /// <summary>Queue §3.6 B2B adjusted/moved events publish onto.</summary>
    public string SapAdjustedOrMovedQueueName { get; init; } = default!;

    /// <summary>Queue §3.7 OMS delta events publish onto.</summary>
    public string OmsDeltaQueueName { get; init; } = default!;

    /// <summary>Queue §3.8 Inventory Comparison Report snapshots publish onto.</summary>
    public string IcrSnapshotQueueName { get; init; } = default!;
}
