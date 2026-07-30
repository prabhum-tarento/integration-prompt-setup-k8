using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

public class CountryDocument : ICosmosDocument
{/// <summary>Deterministic item id.</summary>
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    /// <summary>Cosmos partition key value - <c>SEG_FU_{FulfilmentCode}_{HallmarkCode}</c>.</summary>
    [JsonProperty("category")]
    public string Category { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string RegionCode { get; set; } = default!;
    public string CountryCode { get; set; } = default!;
    public bool IsAX12Market { get; set; }
    [JsonProperty("_etag")]
    public string? ETag { get; set; }
}
