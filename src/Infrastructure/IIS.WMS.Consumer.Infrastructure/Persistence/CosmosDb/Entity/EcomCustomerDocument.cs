using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>Cosmos DB persistence shape for the <see cref="Domain.Aggregates.EcomCustomer"/> Ecom-lookup reference record, shared MasterData container - see <c>EcomCustomerRepository</c>.</summary>
public class EcomCustomerDocument : ICosmosDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    [JsonProperty("category")]
    public string Category { get; set; } = default!;

    public string FulfilmentId { get; set; } = default!;

    public List<string> EcomDcList { get; set; } = [];

    public string? TdcCustomerId { get; set; }

    [JsonProperty("_etag")]
    public string? ETag { get; set; }
}
