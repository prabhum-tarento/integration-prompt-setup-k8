using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

public class FulfilmentUnitDocument : ICosmosDocument
{
    /// <summary>Deterministic item id - the fulfilment id.</summary>
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    /// <summary>Cosmos partition key value - <c>FU_{FulfilmentId}</c>.</summary>
    [JsonProperty("category")]
    public string Category { get; set; } = default!;
    public string CountryCode { get; set; } = default!;
    [JsonProperty("_etag")]
    public string? ETag { get; set; }
}
