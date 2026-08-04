namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>FINISHED-status inventory-adjusted payload published onto <c>InventoryPublishOptions.InventoryAdjustedReflexQueueName</c> (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.5/§9) - a single item line, never a collection, since the originating event carries exactly one.</summary>
public sealed class InventoryAdjustedReflexPublishRequest
{
    public string Channel { get; set; } = default!;
    public string Id { get; set; } = default!;
    public DateTime AdjustmentDate { get; set; }
    public PublishLocation Location { get; set; } = default!;
    public string? Entity { get; set; }
    public string ItemCode { get; set; } = default!;
    public int Quantity { get; set; }
    public string CountryOfOrigin { get; set; } = default!;
    public string HallmarkTo { get; set; } = default!;
    public PublishStateSnapshot ToState { get; set; } = default!;
    public string ReferenceId { get; set; } = default!;
}
