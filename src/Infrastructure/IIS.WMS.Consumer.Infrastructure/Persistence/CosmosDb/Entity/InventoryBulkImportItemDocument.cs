using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>
/// Cosmos DB persistence shape for the bulk-import aggregate (cosmos-db.instructions.md §3). Kept
/// separate from <c>Domain.Aggregates.InventoryBulkImportItem</c> so the Domain layer never
/// references Newtonsoft.Json - only <c>InventoryBulkImportItemMapper</c> and the repository
/// see this type.
/// </summary>
public sealed class InventoryBulkImportItemDocument : ICosmosDocument
{
    /// <summary>Deterministic item id - <c>WarehouseId:Sku</c>.</summary>
    public string Id { get; init; } = default!;

    /// <summary>Warehouse this balance belongs to.</summary>
    public string WarehouseId { get; init; } = default!;

    /// <summary>SKU this balance belongs to.</summary>
    public string Sku { get; init; } = default!;

    /// <summary>Cosmos partition key value - identical to <see cref="Id"/> for this entity.</summary>
    public string Category { get; init; } = default!;

    /// <summary>On-hand quantity as reported by the upstream system.</summary>
    public int Quantity { get; set; }

    /// <summary>Identifies the upstream system that produced this figure.</summary>
    public string SourceSystem { get; init; } = default!;

    /// <summary>Timestamp the upstream system last updated this figure.</summary>
    public DateTime SourceLastUpdatedUtc { get; set; }

    /// <summary>Cosmos's system-managed optimistic-concurrency token. Mapped from <c>_etag</c> since the camelCase naming policy elsewhere can't produce that name.</summary>
    [JsonProperty("_etag")]
    public string? ETag { get; init; }
}
