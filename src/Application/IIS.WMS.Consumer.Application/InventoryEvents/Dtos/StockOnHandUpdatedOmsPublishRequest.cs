namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>§7.3 B2C stock notification payload published onto <c>InventoryPublishOptions.IcrSnapshotQueueName</c> (docs/events/inventory.StockOnHandUpdated.md §9 - shared with the ICR snapshot queue).</summary>
public sealed class StockOnHandUpdatedOmsPublishRequest
{
    public string ProductId { get; set; } = default!;
    public string ProductUnits { get; set; } = default!;
    public PublishLocation Location { get; set; } = default!;
    public string? Entity { get; set; }
    public string? Barcode { get; set; }
    public string Market { get; set; } = default!;
    public string Reason { get; set; } = default!;
    public DateTime UpdatedDate { get; set; }
    public string Channel { get; set; } = default!;
    public IReadOnlyList<StockOnHandUpdatedOmsQuantityDetail> QuantityDetails { get; set; } = [];
}

/// <summary>One quantity entry within a <see cref="StockOnHandUpdatedOmsPublishRequest"/>.</summary>
public sealed class StockOnHandUpdatedOmsQuantityDetail
{
    public int Quantity { get; set; }
    public string State { get; set; } = default!;
    public string Status { get; set; } = default!;
    public string CountryOfOrigin { get; set; } = default!;
    public string Hallmarking { get; set; } = default!;
}
