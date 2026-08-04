using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// §5.3 write-only stock-sync audit record (docs/events/inventory.StockSyncSubmitted.md) - one row per
/// processed state/status via <c>SaveSnapshotDetails</c>, saved whether or not the quantity changed.
/// Like <see cref="MessageArchive"/>, this aggregate has no invariants beyond its required fields and
/// raises no domain events - it is a write-once diagnostic record, not a consistency boundary the rest
/// of the domain reasons about.
/// </summary>
public sealed class SnapshotStockSyncItem : AggregateRoot
{
    /// <summary>Fulfilment unit this snapshot was recorded under - also this entity's Cosmos partition key.</summary>
    public string Category { get; private init; } = default!;

    public string ItemCode { get; private init; } = default!;

    public string CountryOfOriginCode { get; private init; } = default!;

    public string FulfilmentUnit { get; private init; } = default!;

    public string Hallmark { get; private init; } = default!;

    public int Quantity { get; private init; }

    /// <summary>Formatted as <c>"{Domain}.{State}_{Status}"</c> per §5.3.</summary>
    public string QuantityType { get; private init; } = default!;

    /// <summary>
    /// Opaque optimistic-concurrency token populated by the repository. Not guarded on - the
    /// Infrastructure repository upserts unconditionally, since a redelivered message writing the same
    /// snapshot twice under the same deterministic <see cref="AggregateRoot.Id"/> is expected, not a
    /// concurrency conflict to detect.
    /// </summary>
    public string? ETag { get; set; }

    /// <summary>Parameterless so the object initializers in <see cref="Create"/> and <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private SnapshotStockSyncItem()
    {
    }

    /// <summary>
    /// Creates a new snapshot record. <paramref name="id"/> is the caller's own deterministic
    /// identifier, so a redelivered message naturally upserts rather than duplicating.
    /// </summary>
    public static SnapshotStockSyncItem Create(
        string id, string itemCode, string countryOfOriginCode, string fulfilmentUnit,
        string hallmark, int quantity, string quantityType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(fulfilmentUnit);

        return new SnapshotStockSyncItem
        {
            Id = id,
            Category = fulfilmentUnit,
            ItemCode = itemCode,
            CountryOfOriginCode = countryOfOriginCode,
            FulfilmentUnit = fulfilmentUnit,
            Hallmark = hallmark,
            Quantity = quantity,
            QuantityType = quantityType,
        };
    }

    /// <summary>Rehydrates an aggregate from persisted state - the repository mapper's entry point, not for new items.</summary>
    public static SnapshotStockSyncItem Rehydrate(
        string id, string category, string itemCode, string countryOfOriginCode, string fulfilmentUnit,
        string hallmark, int quantity, string quantityType) => new()
    {
        Id = id,
        Category = category,
        ItemCode = itemCode,
        CountryOfOriginCode = countryOfOriginCode,
        FulfilmentUnit = fulfilmentUnit,
        Hallmark = hallmark,
        Quantity = quantity,
        QuantityType = quantityType,
    };
}
