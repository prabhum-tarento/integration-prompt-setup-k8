using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>
/// Cosmos DB persistence shape for a §5.4 stock-sync discrepancy record (docs/events/inventory.StockSyncSubmitted.md).
/// Kept separate from <c>Domain.Aggregates.ItemDiscrepencyDetail</c> so the Domain layer never
/// references Newtonsoft.Json - only <c>ItemDiscrepencyDetailMapper</c> and the repository see this type.
/// </summary>
public sealed class ItemDiscrepencyDetailDocument : ICosmosDocument
{
    /// <summary>Deterministic item id.</summary>
    [JsonProperty("id")]
    public string Id { get; init; } = default!;

    /// <summary>Fulfilment code this discrepancy was recorded under - also this entity's Cosmos partition key.</summary>
    [JsonProperty("category")]
    public string Category { get; init; } = default!;

    public string ItemCode { get; init; } = default!;

    public string CountryOfOrigin { get; init; } = default!;

    public string Hallmark { get; init; } = default!;

    public int IISAvlQty { get; init; }

    public int ReflexAvlQty { get; init; }

    public bool MasterDataExists { get; init; }

    public string FulfilmentCode { get; init; } = default!;

    /// <summary>Cosmos's system-managed optimistic-concurrency token. Mapped from <c>_etag</c>, the fixed name Cosmos's system property always uses.</summary>
    [JsonProperty("_etag")]
    public string? ETag { get; init; }
}
