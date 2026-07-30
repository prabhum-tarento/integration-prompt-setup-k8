using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>§3.5 extended-state inventory snapshot persistence shape - one row per (FulfilmentId, ItemCode, Hallmark, COO, State, Status) combination.</summary>
public class ItemStockInventoryExtendedDocument : ICosmosDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    [JsonProperty("category")]
    public string Category { get; set; } = default!;
    public string ItemCode { get; set; } = default!;
    public string FulfilmentId { get; set; } = default!;
    public string COO { get; set; } = default!;
    public string Hallmark { get; set; } = default!;

    /// <summary>Stored as its enum member name (not the underlying int) so the document stays human-readable in Data Explorer.</summary>
    public string State { get; set; } = default!;

    /// <summary>Stored as its enum member name (not the underlying int) so the document stays human-readable in Data Explorer.</summary>
    public string Status { get; set; } = default!;

    public int? Qty { get; set; }
    public DateTime Timestamp { get; set; }
    public bool? IsPOSM { get; set; }

    [JsonProperty("_etag")]
    public string? ETag { get; set; }
}
