using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>Cosmos DB persistence shape for <see cref="Domain.Aggregates.OrderTracking"/> (docs/events/b2b.sales.ConsolidatedOrderShipped.md §5.4) - read-only here, see <c>OrderTrackingRepository</c>.</summary>
public class OrderTrackingDocument : ICosmosDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    [JsonProperty("category")]
    public string Category { get; set; } = default!;

    public string OrderId { get; set; } = default!;

    public string? CustomerId { get; set; }

    public string? ShipmentId { get; set; }

    public string? Status { get; set; }

    [JsonProperty("_etag")]
    public string? ETag { get; set; }
}
