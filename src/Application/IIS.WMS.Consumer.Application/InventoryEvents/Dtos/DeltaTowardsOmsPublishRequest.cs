namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>§3.7 OMS delta synchronization payload published onto <c>InventoryPublishOptions.OmsDeltaQueueName</c> (docs/InventoryStateChangedFullQueueTrigger.md).</summary>
public sealed class DeltaTowardsOmsPublishRequest
{
    public string ReferenceId { get; set; } = default!;
    public string ProductId { get; set; } = default!;
    public PublishLocation Location { get; set; } = default!;
    public string Reason { get; set; } = default!;
    public DateTime AdjustmentDate { get; set; }
    public string ProductUnits { get; set; } = default!;
    public string Market { get; set; } = default!;
    public IReadOnlyList<DeltaTowardsOmsQuantityDetail> QuantityDetails { get; set; } = [];
}

/// <summary>One quantity-change entry within a <see cref="DeltaTowardsOmsPublishRequest"/>.</summary>
public sealed class DeltaTowardsOmsQuantityDetail
{
    public string CountryOfOrigin { get; set; } = default!;
    public string Hallmarking { get; set; } = default!;
    public int Quantity { get; set; }
    public string State { get; set; } = default!;
    public string Status { get; set; } = default!;
    public IReadOnlyList<string> ReasonTexts { get; set; } = [];
}
