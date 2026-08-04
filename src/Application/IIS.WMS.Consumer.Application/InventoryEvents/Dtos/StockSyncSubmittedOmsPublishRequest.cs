namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>§3.5 OMS B2C stock snapshot payload published onto <c>InventoryPublishOptions.B2CStockQueueName</c> (docs/events/inventory.StockSyncSubmitted.md).</summary>
public sealed class StockSyncSubmittedOmsPublishRequest
{
    public string ProductId { get; set; } = default!;
    public string ProductUnits { get; set; } = default!;
    public PublishLocation Location { get; set; } = default!;
    public string Market { get; set; } = default!;
    public IReadOnlyList<StockSyncSubmittedOmsQuantityDetail> QuantityDetails { get; set; } = [];
}

/// <summary>One quantity entry within a <see cref="StockSyncSubmittedOmsPublishRequest"/> - always B2CAVL reported as AVAILABLE/PICKABLE.</summary>
public sealed class StockSyncSubmittedOmsQuantityDetail
{
    public int Quantity { get; set; }
    public string State { get; set; } = default!;
    public string Status { get; set; } = default!;
}
