using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Domain.Events;
using IIS.WMS.Consumer.Domain.Exceptions;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// Tracks one item's quantity in a single transit status
/// (ALLOCATED/PICKED/INTRANSIT/CREATED) for internal hallmarking
/// (docs/events/inventory.InternalHallmarkingStatusChanged.md §5.2/§6). Because <see cref="Status"/> is
/// part of the composite <see cref="Id"/> itself, a status "transition" is not a mutation of one
/// record's status field - it's an <see cref="IncreaseQuantity"/> on the destination-status record
/// paired with a <see cref="DecreaseQuantity"/> on the source-status record, each a separate
/// <c>ItemStockIntransit</c> document (see §5.2's transition table). Enforces the §6 invariant that
/// in-transit quantity never goes negative.
/// </summary>
public sealed class ItemStockIntransit : AggregateRoot
{
    /// <summary>Item/product code this record tracks.</summary>
    public string ItemCode { get; private init; } = default!;

    /// <summary>Hallmark code (target or source, depending on which status leg this record represents).</summary>
    public string HallmarkCode { get; private init; } = default!;

    /// <summary>ISO 3166-1 alpha-2 country of origin.</summary>
    public string CountryOfOriginCode { get; private init; } = default!;

    /// <summary>Order domain this transit belongs to (e.g. <c>INTERNALHALLMARKING</c>).</summary>
    public string OrderType { get; private init; } = default!;

    /// <summary>Fulfilment location this record belongs to.</summary>
    public string FulfilmentCode { get; private init; } = default!;

    /// <summary>Transit status this record represents (<c>ALLOCATED</c>/<c>PICKED</c>/<c>INTRANSIT</c>/<c>CREATED</c>) - part of the composite key, not a mutable field.</summary>
    public string Status { get; private init; } = default!;

    /// <summary>The composite key (§5.2) - matches the Cosmos partition key, mirroring <see cref="ItemStockInventory.Category"/>.</summary>
    public string Category => Id;

    /// <summary>Quantity currently sitting in this status.</summary>
    public int Quantity { get; private set; }

    /// <summary>UTC timestamp of the most recent state change.</summary>
    public DateTime ModifiedUtc { get; private set; }

    /// <summary>Opaque optimistic-concurrency token populated by the repository from Cosmos's <c>_etag</c>.</summary>
    public string? ETag { get; set; }

    /// <summary>Parameterless so the object initializer in <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private ItemStockIntransit()
    {
    }

    /// <summary>Builds the deterministic composite id/partition key for one item/hallmark/COO/order-type/fulfilment/status combination (§5.2).</summary>
    public static string BuildId(
        string itemCode, string hallmarkCode, string countryOfOriginCode, string orderType, string fulfilmentCode, string status) =>
        $"{itemCode}:{hallmarkCode}:{countryOfOriginCode}:{orderType}:{fulfilmentCode}:{status}".ToUpperInvariant();

    /// <summary>Creates a new zero-initialized transit record - the create-if-missing branch for a status leg that has never been written to.</summary>
    public static ItemStockIntransit CreateDefault(
        string itemCode, string hallmarkCode, string countryOfOriginCode, string orderType, string fulfilmentCode, string status,
        DateTime nowUtc) => new()
    {
        Id = BuildId(itemCode, hallmarkCode, countryOfOriginCode, orderType, fulfilmentCode, status),
        ItemCode = itemCode,
        HallmarkCode = hallmarkCode,
        CountryOfOriginCode = countryOfOriginCode,
        OrderType = orderType,
        FulfilmentCode = fulfilmentCode,
        Status = status,
        ModifiedUtc = nowUtc,
    };

    /// <summary>Rehydrates an aggregate from persisted state - the repository mapper's entry point, not for new aggregates.</summary>
    public static ItemStockIntransit Rehydrate(
        string id, string itemCode, string hallmarkCode, string countryOfOriginCode, string orderType, string fulfilmentCode,
        string status, int quantity, DateTime modifiedUtc) => new()
    {
        Id = id,
        ItemCode = itemCode,
        HallmarkCode = hallmarkCode,
        CountryOfOriginCode = countryOfOriginCode,
        OrderType = orderType,
        FulfilmentCode = fulfilmentCode,
        Status = status,
        Quantity = quantity,
        ModifiedUtc = modifiedUtc,
    };

    /// <summary>
    /// Moves quantity into this status leg - the destination side of a §5.2 transition (e.g. the
    /// <c>PICKED</c> record on an ALLOCATED→PICKED move), or the initial STARTED-status create.
    /// </summary>
    public void IncreaseQuantity(int quantity, DateTime nowUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(quantity);

        Quantity += quantity;
        ModifiedUtc = nowUtc;

        RaiseDomainEvent(new ItemStockIntransitQuantityChanged(Id, ItemCode, Status, quantity));
    }

    /// <summary>
    /// Moves quantity out of this status leg - the source side of a §5.2 transition (e.g. the
    /// <c>ALLOCATED</c> record on an ALLOCATED→PICKED move). Rejects outright rather than clamping when
    /// it would take <see cref="Quantity"/> negative, per §6's "in-transit never decremented below zero"
    /// invariant - <paramref name="quantity"/> exceeding what's on hand is a genuine data-integrity
    /// problem, not tolerable drift.
    /// </summary>
    public void DecreaseQuantity(int quantity, DateTime nowUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(quantity);

        if (quantity > Quantity)
        {
            throw new InvalidItemStockInventoryQtyException(Id, ItemCode, quantity, Quantity - quantity);
        }

        Quantity -= quantity;
        ModifiedUtc = nowUtc;

        RaiseDomainEvent(new ItemStockIntransitQuantityChanged(Id, ItemCode, Status, -quantity));
    }
}
