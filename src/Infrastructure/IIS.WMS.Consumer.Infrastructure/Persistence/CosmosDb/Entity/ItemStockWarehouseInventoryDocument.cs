using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>Cosmos DB persistence shape for <see cref="Domain.Aggregates.ItemStockWarehouseInventory"/> (docs/events/b2b.sales.ConsolidatedOrderShipped.md §5.3) - see <c>ItemStockWarehouseInventoryRepository</c>.</summary>
public class ItemStockWarehouseInventoryDocument : ICosmosDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    [JsonProperty("category")]
    public string Category { get; set; } = default!;

    public string FulfilmentId { get; set; } = default!;

    public string ItemCode { get; set; } = default!;

    public int? Qnty { get; set; }

    public string ModifiedUtc { get; set; } = default!;

    [JsonProperty("_etag")]
    public string? ETag { get; set; }
}
