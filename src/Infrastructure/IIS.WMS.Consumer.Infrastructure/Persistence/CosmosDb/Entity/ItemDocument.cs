using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>Cosmos DB persistence shape for an item-master existence record, shared MasterData container - see <c>ItemRepository</c>.</summary>
public class ItemDocument : ICosmosDocument
{
    /// <summary>Deterministic item id - the item code.</summary>
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    /// <summary>Cosmos partition key value - <c>Item_{ItemCode}</c>.</summary>
    [JsonProperty("category")]
    public string Category { get; set; } = default!;

    [JsonProperty("_etag")]
    public string? ETag { get; set; }
}
