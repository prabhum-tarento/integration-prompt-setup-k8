using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// Tracks per-item engraving warehouse stock for the DEECOMDC e-commerce engraving workflow
/// (docs/events/b2b.sales.ConsolidatedOrderShipped.md §5.3). Point-read by the composite
/// <see cref="FulfilmentId"/>/<see cref="ItemCode"/> key; created on first shipment, incremented
/// thereafter - there is no decrement path for this event.
/// </summary>
public sealed class ItemStockWarehouseInventory : AggregateRoot
{
    /// <summary>Warehouse/fulfilment location this record belongs to.</summary>
    public string FulfilmentId { get; private init; } = default!;

    /// <summary>Item/product code this record tracks.</summary>
    public string ItemCode { get; private init; } = default!;

    /// <summary>The composite key (§5.3) - matches the Cosmos partition key.</summary>
    public string Category => Id;

    /// <summary>Quantity shipped into this fulfilment location for engraving.</summary>
    public int Qnty { get; private set; }

    /// <summary>UTC timestamp of the most recent state change.</summary>
    public DateTime ModifiedUtc { get; private set; }

    /// <summary>Opaque optimistic-concurrency token populated by the repository from Cosmos's <c>_etag</c>.</summary>
    public string? ETag { get; set; }

    /// <summary>Parameterless so the object initializer in <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private ItemStockWarehouseInventory()
    {
    }

    /// <summary>Builds the deterministic composite id/partition key for one fulfilment/item combination (§5.3).</summary>
    public static string BuildId(string fulfilmentId, string itemCode) =>
        $"{fulfilmentId}:{itemCode}".ToUpperInvariant();

    /// <summary>Creates a new record seeded with the first shipment's quantity - the create-if-missing branch (§3.3 step 3).</summary>
    public static ItemStockWarehouseInventory CreateDefault(
        string fulfilmentId, string itemCode, int quantity, DateTime nowUtc) => new()
    {
        Id = BuildId(fulfilmentId, itemCode),
        FulfilmentId = fulfilmentId,
        ItemCode = itemCode,
        Qnty = quantity,
        ModifiedUtc = nowUtc,
    };

    /// <summary>Rehydrates an aggregate from persisted state - the repository mapper's entry point, not for new aggregates.</summary>
    public static ItemStockWarehouseInventory Rehydrate(
        string id, string fulfilmentId, string itemCode, int quantity, DateTime modifiedUtc) => new()
    {
        Id = id,
        FulfilmentId = fulfilmentId,
        ItemCode = itemCode,
        Qnty = quantity,
        ModifiedUtc = modifiedUtc,
    };

    /// <summary>Adds an additional shipment's quantity to this fulfilment/item's engraving stock (§3.3 step 3, found branch).</summary>
    public void IncreaseQuantity(int quantity, DateTime nowUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(quantity);

        Qnty += quantity;
        ModifiedUtc = nowUtc;
    }
}
