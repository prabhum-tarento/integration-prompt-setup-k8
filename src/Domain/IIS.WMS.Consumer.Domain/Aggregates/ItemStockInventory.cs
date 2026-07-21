using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Domain.Events;
using IIS.WMS.Consumer.Domain.Exceptions;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// Consistency boundary for one fulfilment location's item-level B2B/B2C allocated/prepared
/// quantities - ported from the upstream Reflex facade's <c>InventoryPickEventHandler</c>/
/// <c>InventoryUnpickEventHandler</c> (<c>IIS.WMS.Reflex.Application.EventHandlers.InventoryStateChanged</c>).
/// Enforces the oversell-prevention invariant (an allocated/share quantity never goes negative
/// without an explicit reject) per dotnet-architecture-good-practices.instructions.md §5. These
/// records are expected to already exist via bulk import - there is no <c>Create</c> factory,
/// only <see cref="Rehydrate"/>; a missing record is a repository-level "not found", not a Domain
/// concern. <c>Id</c> doubles as the Cosmos partition key (cosmos-db.instructions.md §4), scoped
/// per fulfilment location so the single shared container's point reads stay partition-local.
/// </summary>
public sealed class ItemStockInventory : AggregateRoot
{
    /// <summary>Fulfilment location this record belongs to - the partition scope.</summary>
    public string FulfilmentId { get; private init; } = default!;

    /// <summary>Item/product code this record tracks.</summary>
    public string ItemCode { get; private init; } = default!;

    /// <summary>ISO 3166-1 alpha-2 country of origin.</summary>
    public string CountryOfOrigin { get; private init; } = default!;

    /// <summary>Hallmarking value.</summary>
    public string Hallmark { get; private init; } = default!;

    /// <summary>The composite <c>FulfilmentId:ItemCode:Hallmark:CountryOfOrigin</c> key - matches the Cosmos partition key.</summary>
    public string Category => Id;

    public int B2BAvailable { get; private set; }

    public int B2CAvailable { get; private set; }

    public int B2COriginal { get; private set; }

    public int B2CExtended { get; private set; }

    public int B2CAllocated { get; private set; }

    public int B2BAllocated { get; private set; }

    public int B2CPrepared { get; private set; }

    public int B2BPrepared { get; private set; }

    public int InternalHallmarkAllocated { get; private set; }

    public int InTransit { get; private set; }

    public int B2CThreshold { get; private set; }

    /// <summary>Whether this record participates in B2C extension borrowing against <see cref="B2BUsedShare"/>. Extension recalculation itself is not ported - see docs/InventoryStateChanged-OrderTracking-Relay.md.</summary>
    public bool IsExtended { get; private init; }

    /// <summary>Remaining B2B share a B2C oversell may borrow against, when <see cref="IsExtended"/>.</summary>
    public int B2BUsedShare { get; private set; }

    public int Inspection { get; private init; }

    public int Psc { get; private init; }

    public bool IsPosm { get; private init; }

    /// <summary>UTC timestamp of the most recent state change.</summary>
    public DateTime ModifiedUtc { get; private set; }

    /// <summary>
    /// Opaque optimistic-concurrency token populated by the repository from the store's native
    /// version marker (Cosmos's <c>_etag</c>). Not a business-meaningful field - the aggregate
    /// carries it only so a caller can read-then-write without a second round trip
    /// (cosmos-db.instructions.md §9).
    /// </summary>
    public string? ETag { get; set; }

    /// <summary>Parameterless so the object initializer in <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private ItemStockInventory()
    {
    }

    /// <summary>Builds the deterministic id/partition key for one fulfilment location's item/hallmark/COO combination.</summary>
    public static string BuildId(string fulfilmentId, string itemCode, string hallmark, string countryOfOrigin) =>
        $"{fulfilmentId}:{itemCode}:{hallmark}:{countryOfOrigin}".ToUpperInvariant();

    /// <summary>Rehydrates an aggregate from persisted state - the repository mapper's entry point, not for new aggregates.</summary>
    public static ItemStockInventory Rehydrate(
        string id,
        string fulfilmentId,
        string itemCode,
        string countryOfOrigin,
        string hallmark,
        int b2bAvailable,
        int b2cAvailable,
        int b2cOriginal,
        int b2cExtended,
        int b2cAllocated,
        int b2bAllocated,
        int b2cPrepared,
        int b2bPrepared,
        int internalHallmarkAllocated,
        int inTransit,
        int b2cThreshold,
        bool isExtended,
        int b2bUsedShare,
        int inspection,
        int psc,
        bool isPosm,
        DateTime modifiedUtc) => new()
    {
        Id = id,
        FulfilmentId = fulfilmentId,
        ItemCode = itemCode,
        CountryOfOrigin = countryOfOrigin,
        Hallmark = hallmark,
        B2BAvailable = b2bAvailable,
        B2CAvailable = b2cAvailable,
        B2COriginal = b2cOriginal,
        B2CExtended = b2cExtended,
        B2CAllocated = b2cAllocated,
        B2BAllocated = b2bAllocated,
        B2CPrepared = b2cPrepared,
        B2BPrepared = b2bPrepared,
        InternalHallmarkAllocated = internalHallmarkAllocated,
        InTransit = inTransit,
        B2CThreshold = b2cThreshold,
        IsExtended = isExtended,
        B2BUsedShare = b2bUsedShare,
        Inspection = inspection,
        Psc = psc,
        IsPosm = isPosm,
        ModifiedUtc = modifiedUtc,
    };

    /// <summary>
    /// Applies a B2B pick: moves <paramref name="quantity"/> from allocated into prepared. Mirrors
    /// Reflex's <c>InventoryPickEventHandler</c> <c>PICKEDB2B</c> branch - if allocated would go
    /// negative, it's clamped to zero and flagged rather than rejected (Reflex logs a warning and
    /// continues; this is tolerated data drift, not an invariant violation worth rejecting the
    /// whole pick over).
    /// </summary>
    public void PickB2B(int quantity, DateTime nowUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(quantity);

        var wasClamped = quantity > B2BAllocated;
        B2BAllocated = Math.Max(0, B2BAllocated - quantity);
        B2BPrepared += quantity;
        ModifiedUtc = nowUtc;

        if (IsExtended)
        {
            B2BUsedShare = Math.Max(0, B2BUsedShare - quantity);
        }

        RaiseDomainEvent(new ItemStockPicked(Id, FulfilmentId, ItemCode, "B2B", quantity, wasClamped));
    }

    /// <summary>
    /// Applies a B2C pick: increments prepared, then decrements allocated if enough is available.
    /// Mirrors Reflex's <c>InventoryPickEventHandler</c> <c>PICKEDB2C</c> branch: a non-extended
    /// oversell throws <see cref="InsufficientItemStockException"/> (a real invariant violation);
    /// an extended oversell instead borrows the shortfall from <see cref="B2BUsedShare"/>, throwing
    /// <see cref="ItemStockShareExhaustedException"/> if that would also go negative. The
    /// B2CExtended/B2CAVL recalculation Reflex performs afterward via
    /// <c>CalculateB2CExtensionAsync</c> is not ported - see docs/InventoryStateChanged-OrderTracking-Relay.md.
    /// </summary>
    public void PickB2C(int quantity, DateTime nowUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(quantity);

        B2CPrepared += quantity;

        if (B2CAllocated >= quantity)
        {
            B2CAllocated -= quantity;
            ModifiedUtc = nowUtc;

            RaiseDomainEvent(new ItemStockPicked(Id, FulfilmentId, ItemCode, "B2C", quantity, WasClamped: false));

            return;
        }

        if (!IsExtended)
        {
            throw new InsufficientItemStockException(Id, ItemCode, quantity, B2CAllocated);
        }

        var shortfall = quantity - B2CAllocated;

        if (shortfall > B2BUsedShare)
        {
            throw new ItemStockShareExhaustedException(Id, ItemCode, shortfall, B2BUsedShare);
        }

        B2CAllocated = 0;
        B2BUsedShare -= shortfall;
        ModifiedUtc = nowUtc;

        RaiseDomainEvent(new ItemStockPicked(Id, FulfilmentId, ItemCode, "B2C", quantity, WasClamped: false));
    }

    /// <summary>
    /// Applies an unpick (<c>Dgp</c>): moves <paramref name="quantity"/> out of B2B prepared.
    /// Mirrors Reflex's <c>InventoryUnpickEventHandler</c> <c>DGP</c> branch - rejects outright
    /// (rather than clamping) when nothing is prepared, since an unpick with no prior pick is a
    /// genuine invariant violation, not tolerable drift.
    /// </summary>
    public void Unpick(int quantity, DateTime nowUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(quantity);

        if (B2BPrepared == 0)
        {
            throw new InsufficientItemStockException(Id, ItemCode, quantity, B2BPrepared);
        }

        B2BPrepared -= quantity;
        ModifiedUtc = nowUtc;

        RaiseDomainEvent(new ItemStockUnpicked(Id, FulfilmentId, ItemCode, quantity));
    }
}