using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// One warehouse/SKU's on-hand quantity as reported by an upstream bulk-import feed
/// (integration-resiliency.instructions.md §1, bulk-import consumer). Unlike
/// <see cref="InventoryEvent"/>, this is not a reserve/allocate state machine - a bulk import
/// unconditionally overwrites the current on-hand figure for its warehouse/SKU, so this aggregate
/// has no invariants beyond non-negative quantity and no domain events to raise. Deliberately
/// separate from <see cref="InventoryEvent"/> rather than reusing it - forcing an unordered,
/// last-write-wins bulk feed through the reserve/allocate invariants built for concurrent
/// reservation arithmetic would misuse them.
/// </summary>
public sealed class InventoryBulkImportItem : AggregateRoot
{
    /// <summary>The warehouse this on-hand balance belongs to - the first half of the composite partition key.</summary>
    public string WarehouseId { get; private init; } = default!;

    /// <summary>The SKU this on-hand balance tracks - the second half of the composite partition key.</summary>
    public string Sku { get; private init; } = default!;

    /// <summary>The composite <c>WarehouseId:Sku</c> key - matches the Cosmos partition key.</summary>
    public string Category => $"{WarehouseId}:{Sku}";

    /// <summary>On-hand quantity as reported by the upstream system. Never negative.</summary>
    public int Quantity { get; private set; }

    /// <summary>Identifies the upstream system that produced this bulk-import event.</summary>
    public string SourceSystem { get; private init; } = default!;

    /// <summary>Timestamp the upstream system last updated this figure - not when this consumer processed it.</summary>
    public DateTime SourceLastUpdatedUtc { get; private set; }

    /// <summary>
    /// Opaque optimistic-concurrency token populated by the repository. Carried for parity with
    /// <see cref="InventoryEvent"/> and possible future use, but the bulk-import write path
    /// deliberately does not guard on it - the Infrastructure repository upserts unconditionally,
    /// unlike the ETag-guarded replace <see cref="InventoryEvent"/> requires, since this data is an
    /// unordered, idempotent snapshot reload rather than a concurrently-contested balance.
    /// </summary>
    public string? ETag { get; set; }

    /// <summary>Parameterless so the object initializers in <see cref="Create"/> and <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private InventoryBulkImportItem()
    {
    }

    /// <summary>
    /// Creates (or represents the next overwrite of) a bulk-import item. <paramref name="warehouseId"/>/
    /// <paramref name="sku"/> derive the deterministic id, so redelivery of the same warehouse/SKU
    /// naturally upserts rather than duplicating.
    /// </summary>
    public static InventoryBulkImportItem Create(
        string warehouseId, string sku, int quantity, string sourceSystem, DateTime sourceLastUpdatedUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(warehouseId);
        ArgumentException.ThrowIfNullOrWhiteSpace(sku);
        ArgumentOutOfRangeException.ThrowIfNegative(quantity);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceSystem);

        return new InventoryBulkImportItem
        {
            Id = $"{warehouseId}:{sku}",
            WarehouseId = warehouseId,
            Sku = sku,
            Quantity = quantity,
            SourceSystem = sourceSystem,
            SourceLastUpdatedUtc = sourceLastUpdatedUtc,
        };
    }

    /// <summary>Rehydrates an aggregate from persisted state - the repository mapper's entry point, not for new items.</summary>
    public static InventoryBulkImportItem Rehydrate(
        string id, string warehouseId, string sku, int quantity, string sourceSystem, DateTime sourceLastUpdatedUtc) => new()
    {
        Id = id,
        WarehouseId = warehouseId,
        Sku = sku,
        Quantity = quantity,
        SourceSystem = sourceSystem,
        SourceLastUpdatedUtc = sourceLastUpdatedUtc,
    };
}
