namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>§3.6 B2B adjusted/moved event payload published onto <c>InventoryPublishOptions.SapAdjustedOrMovedQueueName</c> (docs/InventoryStateChangedFullQueueTrigger.md).</summary>
public sealed class InventoryAdjustedOrMovedPublishRequest
{
    public string Channel { get; set; } = default!;
    public string Id { get; set; } = default!;
    public DateTime AdjustmentDate { get; set; }
    public PublishLocation Location { get; set; } = default!;
    public string? Entity { get; set; }
    public PublishStateSnapshot FromState { get; set; } = default!;
    public PublishStateSnapshot ToState { get; set; } = default!;
    public string ReferenceId { get; set; } = default!;
    public string InventoryEventType { get; set; } = default!;
    public string? Reason { get; set; }
    public IReadOnlyList<InventoryAdjustedOrMovedLine> Lines { get; set; } = [];
}

/// <summary>One item line within an <see cref="InventoryAdjustedOrMovedPublishRequest"/>.</summary>
public sealed class InventoryAdjustedOrMovedLine
{
    public string ItemCode { get; set; } = default!;
    public int Qty { get; set; }
    public string CountryOfOrigin { get; set; } = default!;
    public string Hallmarking { get; set; } = default!;
    public string? Reason { get; set; }
}
