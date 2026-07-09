using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>
/// Cosmos DB persistence shape for the inventory aggregate (cosmos-db.instructions.md §3). Kept
/// separate from <c>Domain.Aggregates.InventoryEvent</c> so the Domain layer never references
/// Newtonsoft.Json - only <see cref="InventoryEventMapper"/> and the repository see this type.
/// </summary>
public sealed class InventoryEventDocument : ICosmosDocument
{
    /// <summary>Deterministic item id - <c>WarehouseId:Sku</c>.</summary>
    public string Id { get; init; } = default!;

    /// <summary>Warehouse this balance belongs to.</summary>
    public string WarehouseId { get; init; } = default!;

    /// <summary>SKU this balance belongs to.</summary>
    public string Sku { get; init; } = default!;

    // Composite partition key per §4 - kept as its own property (not derived at query time) so
    // every write and query uses the identical value.
    /// <summary>Cosmos partition key value - identical to <see cref="Id"/> for this entity.</summary>
    public string PartitionKey { get; init; } = default!;

    /// <summary>Current on-hand quantity, net of active reservations.</summary>
    public int OnHandQuantity { get; set; }

    /// <summary>Reservations that have decremented on-hand but not yet been allocated or released, keyed by reservation id.</summary>
    public Dictionary<string, int> ActiveReservations { get; init; } = [];

    /// <summary>UTC timestamp the item was first created.</summary>
    public DateTime CreatedUtc { get; init; }

    /// <summary>UTC timestamp of the most recent state change.</summary>
    public DateTime ModifiedUtc { get; set; }

    /// <summary>Cosmos's system-managed optimistic-concurrency token. Mapped from <c>_etag</c> since the camelCase naming policy elsewhere can't produce that name.</summary>
    [JsonProperty("_etag")]
    public string? ETag { get; init; }
}
