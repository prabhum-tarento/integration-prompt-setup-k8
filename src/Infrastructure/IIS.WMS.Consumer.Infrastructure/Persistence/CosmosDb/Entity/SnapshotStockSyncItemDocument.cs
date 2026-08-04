using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>
/// Cosmos DB persistence shape for a §5.3 stock-sync snapshot record (docs/events/inventory.StockSyncSubmitted.md).
/// Kept separate from <c>Domain.Aggregates.SnapshotStockSyncItem</c> so the Domain layer never
/// references Newtonsoft.Json - only <c>SnapshotStockSyncItemMapper</c> and the repository see this type.
/// </summary>
public sealed class SnapshotStockSyncItemDocument : ICosmosDocument
{
    /// <summary>Deterministic item id.</summary>
    [JsonProperty("id")]
    public string Id { get; init; } = default!;

    /// <summary>Fulfilment unit this record was recorded under - also this entity's Cosmos partition key.</summary>
    [JsonProperty("category")]
    public string Category { get; init; } = default!;

    public string ItemCode { get; init; } = default!;

    public string CountryOfOriginCode { get; init; } = default!;

    public string FulfilmentUnit { get; init; } = default!;

    public string Hallmark { get; init; } = default!;

    public int Quantity { get; init; }

    public string QuantityType { get; init; } = default!;

    /// <summary>Cosmos's system-managed optimistic-concurrency token. Mapped from <c>_etag</c>, the fixed name Cosmos's system property always uses.</summary>
    [JsonProperty("_etag")]
    public string? ETag { get; init; }
}
