namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>§3.8 Inventory Comparison Report snapshot payload published onto <c>InventoryPublishOptions.IcrSnapshotQueueName</c> (docs/InventoryStateChangedFullQueueTrigger.md).</summary>
public sealed class OmniInventoryAvailabilityPublishRequest
{
    public DateTime ReportDate { get; set; }
    public PublishLocation Location { get; set; } = default!;
    public string ProductId { get; set; } = default!;
    public string ProductUnits { get; set; } = default!;
    public string CountryOfOrigin { get; set; } = default!;
    public string Hallmarking { get; set; } = default!;
    public IReadOnlyList<OmniInventoryQuantityDetail> QuantityDetails { get; set; } = [];
}

/// <summary>One state/domain quantity entry within an <see cref="OmniInventoryAvailabilityPublishRequest"/> - B2B_AVL/B2C_AVL/B2B_PREP/B2C_PREP.</summary>
public sealed class OmniInventoryQuantityDetail
{
    public int Quantity { get; set; }
    public string State { get; set; } = default!;
    public string Status { get; set; } = default!;
    public string Domain { get; set; } = default!;
}
