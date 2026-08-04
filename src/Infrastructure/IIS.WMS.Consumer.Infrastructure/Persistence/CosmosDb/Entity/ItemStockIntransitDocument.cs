using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>Persistence shape for <see cref="Domain.Aggregates.ItemStockIntransit"/> (docs/events/inventory.InternalHallmarkingStatusChanged.md §5.2) - one document per item/hallmark/COO/order-type/fulfilment/status combination.</summary>
public class ItemStockIntransitDocument : ICosmosDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    [JsonProperty("category")]
    public string Category { get; set; } = default!;

    public string ItemCode { get; set; } = default!;
    public string HallmarkCode { get; set; } = default!;
    public string CountryOfOriginCode { get; set; } = default!;
    public string OrderType { get; set; } = default!;
    public string FulfilmentCode { get; set; } = default!;
    public string Status { get; set; } = default!;
    public int? Quantity { get; set; }
    public string Timestamp { get; set; } = default!;

    [JsonProperty("_etag")]
    public string? ETag { get; set; }
}
