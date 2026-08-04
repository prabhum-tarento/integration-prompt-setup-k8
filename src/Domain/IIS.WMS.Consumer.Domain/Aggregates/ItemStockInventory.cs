using System.Net.NetworkInformation;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Domain.Enums;
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

    /// <summary>
    /// BR-only sellable quantity reported under the <c>AVAILABLETOSELL</c> state
    /// (docs/events/inventory.StockSyncSubmitted.md §3.2/§4.2/§5.1) - <see langword="null"/> for
    /// fulfilment codes that never report this state, distinct from a genuine reported zero.
    /// </summary>
    public int? B2CAvailableToSell { get; private set; }

    public int InternalHallmarkAllocated { get; private set; }

    public int InTransit { get; private set; }

    public int B2CThreshold { get; private set; }

    /// <summary>Whether this record participates in B2C extension borrowing against <see cref="B2BUsedShare"/>.</summary>
    public bool IsExtended { get; private set; }

    /// <summary>Remaining B2B share a B2C oversell may borrow against, when <see cref="IsExtended"/>.</summary>
    public int B2BUsedShare { get; private set; }

    public int Inspection { get; private init; }

    public int Psc { get; private set; }

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

    /// <summary>
    /// Creates a new zero-initialized record for a fulfilment location/item/hallmark/COO combination
    /// that has no existing <c>ItemStockInventory</c> row yet - mirrors the upstream Reflex facade's
    /// <c>InventorySegmentationAndExtensionHandler</c> create-if-missing branch (see
    /// docs/events/inventory.InventoryStateChanged.md §3.3), which zero-initializes every quantity
    /// field rather than leaving the row absent.
    /// </summary>
    public static ItemStockInventory CreateDefault(
        string fulfilmentId, string itemCode, string hallmark, string countryOfOrigin, DateTime nowUtc) => new()
    {
        Id = BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin),
        FulfilmentId = fulfilmentId,
        ItemCode = itemCode,
        CountryOfOrigin = countryOfOrigin,
        Hallmark = hallmark,
        ModifiedUtc = nowUtc,
    };

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
        DateTime modifiedUtc,
        int? b2cAvailableToSell = null) => new()
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
        B2CAvailableToSell = b2cAvailableToSell,
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
    /// <c>CalculateB2CExtensionAsync</c> is not ported - see docs/events/shared/b2c-extension-calculation.md.
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
    /// Applies an unpick (<c>Dgp</c>): reverses a prior pick by moving <paramref name="quantity"/>
    /// out of B2B prepared and back into B2B allocated. Mirrors Reflex's
    /// <c>InventoryUnpickEventHandler</c> <c>DGP</c> branch - rejects outright (rather than clamping)
    /// when nothing is prepared, since an unpick with no prior pick is a genuine invariant
    /// violation, not tolerable drift.
    /// </summary>
    public void Unpick(int quantity, DateTime nowUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(quantity);

        if (B2BPrepared == 0)
        {
            throw new InsufficientItemStockException(Id, ItemCode, quantity, B2BPrepared);
        }

        B2BPrepared -= quantity;
        B2BAllocated += quantity;
        ModifiedUtc = nowUtc;

        RaiseDomainEvent(new ItemStockUnpicked(Id, FulfilmentId, ItemCode, quantity));
    }

    /// <summary>
    /// Recalculates <see cref="B2CExtended"/> as the actual B2B available quantity - mirrors the
    /// upstream Reflex facade's <c>FormulaHelper.CalculateActualB2BAvailable</c> exactly
    /// (docs/events/inventory.InventoryStateChanged.md §3.4): <c>B2BAvailable - (B2BAllocated + B2BUsedShare
    /// + B2BPrepared)</c>, clamped at zero. There is no store-leverage multiplier in this formula -
    /// leverage only gates *whether* extension applies at all (see <see cref="ActivateExtension"/>),
    /// resolved upstream of this call.
    /// </summary>
    public void CalculateB2CExtended()
    {
        B2CExtended = Math.Max(0, B2BAvailable - (B2BAllocated + B2BUsedShare + B2BPrepared));
    }

    /// <summary>
    /// Recalculates B2C available quantity by combining the original B2C allocation with
    /// any B2C extension amount. Returns the new calculated value without modifying the aggregate.
    /// </summary>
    public int CalculateB2CAvailable()
    {
        return B2COriginal + B2CExtended;
    }

    /// <summary>
    /// Updates the B2C available quantity after extension recalculation. Called by the
    /// extension calculation helper when the result differs from the previous value.
    /// </summary>
    public void UpdateB2CAvailable(int newB2CAvailable)
    {
        B2CAvailable = newB2CAvailable;
    }

    /// <summary>
    /// Marks this record as participating in B2C extension borrowing - mirrors the upstream Reflex
    /// facade's <c>itemStockInventoryDTO.IsExtended = true;</c> flip immediately before
    /// <c>DoItemLevelExtension</c> (docs/events/inventory.InventoryStateChanged.md §3.3). A one-way
    /// flag flip; there is no corresponding deactivation path in the ported trigger.
    /// </summary>
    public void ActivateExtension() => IsExtended = true;

    /// <summary>
    /// §3.3 fulfilment-level B2C-only segmentation for third-party-logistics locations - mirrors
    /// the upstream Reflex facade's <c>SegmentInventoryHelper.DoFulfilmentLevelB2CSegmentation</c>
    /// exactly, including its clamp-to-zero (never reject) behavior on a negative inbound quantity
    /// that would oversell <see cref="B2CAvailable"/>.
    /// </summary>
    public void DoFulfilmentLevelB2CSegmentation(int inboundQty, DateTime nowUtc)
    {
        if (inboundQty < 0)
        {
            var actualB2CAvailable = B2CAvailable - (B2CAllocated + B2CPrepared);
            var abs = Math.Abs(inboundQty);

            B2CAvailable = actualB2CAvailable - abs >= 0
                ? B2CAvailable - abs
                : Math.Max(0, B2CAvailable - abs);
        }
        else if (inboundQty > 0)
        {
            B2CAvailable += inboundQty;
        }

        ModifiedUtc = nowUtc;
    }

    /// <summary>
    /// §3.3 fulfilment-level segmentation for the non-extended, non-3PL fallback path - mirrors the
    /// upstream Reflex facade's <c>SegmentInventoryHelper.DoFulfilmentLevelSegmentation</c> exactly:
    /// all inbound movement (positive or negative) lands on <see cref="B2BAvailable"/>, clamped to
    /// zero on oversell rather than rejected. Does not touch <see cref="B2CAvailable"/> - this path
    /// never changes the OMS-facing delta (docs/events/inventory.InventoryStateChanged.md §3.3).
    /// </summary>
    public void DoFulfilmentLevelSegmentation(int inboundQty, DateTime nowUtc)
    {
        if (inboundQty < 0)
        {
            var actualB2BAvailable = B2BAvailable - (B2BAllocated + B2BUsedShare + B2BPrepared);
            var abs = Math.Abs(inboundQty);

            B2BAvailable = actualB2BAvailable - abs >= 0
                ? B2BAvailable - abs
                : Math.Max(0, B2BAvailable - abs);
        }
        else if (inboundQty > 0)
        {
            B2BAvailable += inboundQty;
        }

        ModifiedUtc = nowUtc;
    }

    /// <summary>
    /// §3.3 item-level extension - mirrors the upstream Reflex facade's
    /// <c>ExtendInventoryHelper.DoItemLevelExtension</c> exactly. Only applies when
    /// <see cref="IsExtended"/> (set via <see cref="ActivateExtension"/> immediately before this
    /// call, per the ported trigger's own sequencing) - a no-op otherwise, matching Reflex's
    /// early-exit. <paramref name="ecomShare"/> is the item-level segmentation rule's configured
    /// e-commerce share threshold.
    /// </summary>
    public void DoItemLevelExtension(int inboundQty, int ecomShare, DateTime nowUtc)
    {
        if (!IsExtended)
        {
            return;
        }

        if (inboundQty > 0)
        {
            var actualB2COriginal = B2COriginal - B2CAllocated - B2CPrepared;

            if (ecomShare - actualB2COriginal >= inboundQty)
            {
                B2COriginal += inboundQty;
            }
            else
            {
                var b2cShare = ecomShare - actualB2COriginal;
                B2BAvailable += inboundQty - b2cShare;
                B2COriginal += b2cShare;
                CalculateB2CExtended();
            }
        }
        else
        {
            var abs = Math.Abs(inboundQty);
            var actualB2BAvailable = B2BAvailable - (B2BAllocated + B2BUsedShare + B2BPrepared);

            if (actualB2BAvailable - abs >= 0)
            {
                B2BAvailable -= abs;
                CalculateB2CExtended();
            }
            else
            {
                var b2cShare = abs - actualB2BAvailable;
                var b2bShare = abs - b2cShare;
                B2COriginal -= b2cShare;
                B2BAvailable -= b2bShare;
                CalculateB2CExtended();
            }
        }

        B2CAvailable = B2CExtended + B2COriginal;
        ModifiedUtc = nowUtc;
    }

    /// <summary>
    /// §3.2 stock-sync Set: overwrites (never increments) the B2C sellable quantities with the
    /// values reported by this sync, per docs/events/inventory.StockSyncSubmitted.md §3.2/§5.1 -
    /// unlike every other mutator on this aggregate, which applies a delta. <paramref name="b2cAvailableToSell"/>
    /// is <see langword="null"/> for fulfilment codes that never report the BR-only
    /// <c>AVAILABLETOSELL</c> state (§4.2), left untouched in that case rather than cleared, so a
    /// non-BR sync never wipes out a value only a BR sync would have set.
    /// </summary>
    public void ApplyStockSync(int b2cAvl, int b2cPrepared, int? b2cAvailableToSell, DateTime nowUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(b2cAvl);
        ArgumentOutOfRangeException.ThrowIfNegative(b2cPrepared);

        B2CAvailable = b2cAvl;
        B2CPrepared = b2cPrepared;

        if (b2cAvailableToSell is not null)
        {
            B2CAvailableToSell = b2cAvailableToSell;
        }

        ModifiedUtc = nowUtc;

        RaiseDomainEvent(new ItemStockSyncApplied(Id, FulfilmentId, ItemCode, B2CAvailable, B2CPrepared, B2CAvailableToSell));
    }

    /// <summary>
    /// Internal-hallmarking STARTED-status allocation - mirrors the upstream Reflex facade's
    /// <c>orderToInventoryAllocatedEventAsync</c> exactly
    /// (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.1). <paramref name="quantity"/>
    /// is signed (allocate on positive, undo-allocate on negative) - a zero quantity is a business
    /// rejection (nothing to allocate), and a resulting negative <see cref="B2BAllocated"/> is
    /// likewise rejected outright rather than clamped, per the plan's explicit "throws" decision for
    /// this path (unlike <see cref="PickB2B"/>'s tolerated-drift clamp).
    /// </summary>
    public void AllocateInternalHallmarking(int quantity, DateTime nowUtc)
    {
        if (quantity == 0)
        {
            throw new InvalidItemStockInventoryQtyException(Id, ItemCode, quantity, B2BAllocated);
        }

        var newB2BAllocated = B2BAllocated + quantity;

        if (newB2BAllocated < 0)
        {
            throw new InvalidItemStockInventoryQtyException(Id, ItemCode, quantity, newB2BAllocated);
        }

        if (B2CAllocated > B2CAvailable)
        {
            throw new InvalidItemStockInventoryQtyException(Id, ItemCode, quantity, B2CAllocated);
        }

        B2BAllocated = newB2BAllocated;
        ModifiedUtc = nowUtc;

        if (IsExtended)
        {
            CalculateB2CExtended();
            UpdateB2CAvailable(CalculateB2CAvailable());
        }

        RaiseDomainEvent(new InternalHallmarkingAllocated(Id, FulfilmentId, ItemCode, quantity));
    }

    /// <summary>
    /// Internal-hallmarking PICKED-status consolidated-shipment logic - mirrors the upstream Reflex
    /// facade's <c>consolidatedOrderShippedEventHandlerAsync</c> exactly, applying the §3.3 three-branch
    /// table by <paramref name="confirmationType"/>: <c>PRELIMINARY</c> only accrues <see cref="Psc"/>
    /// (tentative shipment, nothing else moves yet); <c>STANDARD_FOLLOWING_PRELIMINARY</c> finalizes a
    /// prior preliminary shipment (decrements <see cref="B2BAvailable"/>/<see cref="B2BPrepared"/>/
    /// <see cref="Psc"/> together); anything else is a direct shipment (decrements
    /// <see cref="B2BAvailable"/>/<see cref="B2BPrepared"/> only). All decrements clamp to zero rather
    /// than reject, per §3.3's own validation rule.
    /// </summary>
    public void ApplyConsolidatedShipment(ConfirmationType confirmationType, int shippedQuantity, DateTime nowUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shippedQuantity);

        switch (confirmationType)
        {
            case ConfirmationType.PRELIMINARY:
                Psc += shippedQuantity;
                break;

            case ConfirmationType.STANDARD_FOLLOWING_PRELIMINARY:
                B2BAvailable = Math.Max(0, B2BAvailable - shippedQuantity);
                B2BPrepared = Math.Max(0, B2BPrepared - shippedQuantity);
                Psc = Math.Max(0, Psc - shippedQuantity);
                break;

            default:
                B2BAvailable = Math.Max(0, B2BAvailable - shippedQuantity);
                B2BPrepared = Math.Max(0, B2BPrepared - shippedQuantity);
                break;
        }

        ModifiedUtc = nowUtc;

        if (IsExtended)
        {
            CalculateB2CExtended();
            UpdateB2CAvailable(CalculateB2CAvailable());
        }

        RaiseDomainEvent(new InternalHallmarkingShipped(Id, FulfilmentId, ItemCode, confirmationType.ToString(), shippedQuantity));
    }

    /// <summary>
    /// Internal-hallmarking FINISHED-status transit completion - mirrors the upstream Reflex facade's
    /// "transition from in-transit to available in target hallmark" step
    /// (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.5/§6): moves
    /// <paramref name="quantity"/> out of <see cref="InTransit"/> and into <see cref="B2BAvailable"/> on
    /// this (the <c>HallmarkTo</c>) record. Rejects outright rather than clamping when it would take
    /// <see cref="InTransit"/> negative, per §6's "in-transit never decremented below zero" invariant.
    /// </summary>
    public void CompleteInternalHallmarkingTransit(int quantity, DateTime nowUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(quantity);

        if (quantity > InTransit)
        {
            throw new InvalidItemStockInventoryQtyException(Id, ItemCode, quantity, InTransit - quantity);
        }

        InTransit -= quantity;
        B2BAvailable += quantity;
        ModifiedUtc = nowUtc;

        RaiseDomainEvent(new InternalHallmarkingTransitCompleted(Id, FulfilmentId, ItemCode, quantity));
    }
}