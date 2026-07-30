using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Events;
using IIS.WMS.Consumer.Domain.Exceptions;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>
/// Business-rule tests for the <see cref="ItemStockInventory"/> aggregate - B2B/B2C pick, unpick,
/// oversell prevention, and B2C-extension borrowing (ported from the upstream Reflex facade's
/// <c>InventoryPickEventHandler</c>/<c>InventoryUnpickEventHandler</c>).
/// </summary>
public class ItemStockInventoryTests
{
    private static readonly DateTime Now = new(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc);

    private static ItemStockInventory CreateAggregate(
        int b2bAllocated = 10, int b2cAllocated = 10, bool isExtended = false, int b2bUsedShare = 0,
        int b2bAvailable = 20, int b2cAvailable = 20, int b2cOriginal = 20, int b2cExtended = 0,
        int b2cPrepared = 0, int b2bPrepared = 0) =>
        ItemStockInventory.Rehydrate(
            id: "WH1:SKU1:925:TH",
            fulfilmentId: "WH1",
            itemCode: "SKU1",
            countryOfOrigin: "TH",
            hallmark: "925",
            b2bAvailable: b2bAvailable,
            b2cAvailable: b2cAvailable,
            b2cOriginal: b2cOriginal,
            b2cExtended: b2cExtended,
            b2cAllocated: b2cAllocated,
            b2bAllocated: b2bAllocated,
            b2cPrepared: b2cPrepared,
            b2bPrepared: b2bPrepared,
            internalHallmarkAllocated: 0,
            inTransit: 0,
            b2cThreshold: 0,
            isExtended: isExtended,
            b2bUsedShare: b2bUsedShare,
            inspection: 0,
            psc: 0,
            isPosm: false,
            modifiedUtc: Now);

    [Fact(DisplayName = "PickB2B decrements allocated and increments prepared when enough is available")]
    public void PickB2B_SufficientAllocated_MovesQuantityFromAllocatedToPrepared()
    {
        var aggregate = CreateAggregate(b2bAllocated: 10);

        aggregate.PickB2B(4, Now);

        Assert.Equal(6, aggregate.B2BAllocated);
        Assert.Equal(4, aggregate.B2BPrepared);
        var raised = Assert.IsType<ItemStockPicked>(Assert.Single(aggregate.DomainEvents));
        Assert.Equal("B2B", raised.Channel);
        Assert.Equal(4, raised.Quantity);
        Assert.False(raised.WasClamped);
    }

    [Fact(DisplayName = "PickB2B clamps allocated to zero and flags WasClamped when the request exceeds allocated")]
    public void PickB2B_QuantityExceedsAllocated_ClampsToZeroAndFlagsWasClamped()
    {
        var aggregate = CreateAggregate(b2bAllocated: 3);

        aggregate.PickB2B(5, Now);

        Assert.Equal(0, aggregate.B2BAllocated);
        Assert.Equal(5, aggregate.B2BPrepared);
        var raised = Assert.IsType<ItemStockPicked>(Assert.Single(aggregate.DomainEvents));
        Assert.True(raised.WasClamped);
    }

    [Fact(DisplayName = "PickB2C decrements allocated and increments prepared when enough is available")]
    public void PickB2C_SufficientAllocated_MovesQuantityFromAllocatedToPrepared()
    {
        var aggregate = CreateAggregate(b2cAllocated: 10);

        aggregate.PickB2C(4, Now);

        Assert.Equal(6, aggregate.B2CAllocated);
        Assert.Equal(4, aggregate.B2CPrepared);
        var raised = Assert.IsType<ItemStockPicked>(Assert.Single(aggregate.DomainEvents));
        Assert.Equal("B2C", raised.Channel);
    }

    [Fact(DisplayName = "PickB2C throws InsufficientItemStockException on a non-extended oversell")]
    public void PickB2C_OversellWithoutExtension_ThrowsInsufficientItemStockException()
    {
        var aggregate = CreateAggregate(b2cAllocated: 3, isExtended: false);

        var exception = Assert.Throws<InsufficientItemStockException>(() => aggregate.PickB2C(5, Now));

        Assert.Equal(5, exception.Requested);
        Assert.Equal(3, exception.Available);
        Assert.Empty(aggregate.DomainEvents);
    }

    [Fact(DisplayName = "PickB2C borrows the shortfall from B2BUsedShare on an extended oversell")]
    public void PickB2C_OversellWithExtensionAndSufficientShare_BorrowsFromB2BUsedShare()
    {
        var aggregate = CreateAggregate(b2cAllocated: 3, isExtended: true, b2bUsedShare: 10);

        aggregate.PickB2C(5, Now);

        Assert.Equal(0, aggregate.B2CAllocated);
        Assert.Equal(5, aggregate.B2CPrepared);
        Assert.Equal(8, aggregate.B2BUsedShare);
        var raised = Assert.IsType<ItemStockPicked>(Assert.Single(aggregate.DomainEvents));
        Assert.Equal("B2C", raised.Channel);
    }

    [Fact(DisplayName = "PickB2C throws ItemStockShareExhaustedException when the extended borrow would exceed B2BUsedShare")]
    public void PickB2C_OversellWithExtensionAndInsufficientShare_ThrowsItemStockShareExhaustedException()
    {
        var aggregate = CreateAggregate(b2cAllocated: 3, isExtended: true, b2bUsedShare: 1);

        var exception = Assert.Throws<ItemStockShareExhaustedException>(() => aggregate.PickB2C(5, Now));

        Assert.Equal(2, exception.Requested);
        Assert.Equal(1, exception.AvailableShare);
        Assert.Empty(aggregate.DomainEvents);
    }

    [Fact(DisplayName = "Unpick decrements B2BPrepared, re-increments B2BAllocated, and raises ItemStockUnpicked")]
    public void Unpick_PreparedQuantityAvailable_DecrementsB2BPreparedIncrementsB2BAllocatedAndRaisesItemStockUnpicked()
    {
        var aggregate = CreateAggregate(b2bAllocated: 10);
        aggregate.PickB2B(6, Now);

        aggregate.Unpick(4, Now);

        Assert.Equal(2, aggregate.B2BPrepared);
        Assert.Equal(8, aggregate.B2BAllocated);
        Assert.Contains(aggregate.DomainEvents, e => e is ItemStockUnpicked);
    }

    [Fact(DisplayName = "Unpick throws InsufficientItemStockException when nothing is prepared")]
    public void Unpick_NothingPrepared_ThrowsInsufficientItemStockException()
    {
        var aggregate = CreateAggregate();

        var exception = Assert.Throws<InsufficientItemStockException>(() => aggregate.Unpick(1, Now));

        Assert.Equal(0, exception.Available);
    }

    [Fact(DisplayName = "Category returns the same value as Id, matching the Cosmos partition key")]
    public void Category_ReturnsId()
    {
        var aggregate = CreateAggregate();

        Assert.Equal(aggregate.Id, aggregate.Category);
    }

    [Fact(DisplayName = "BuildId composes a deterministic upper-invariant composite key")]
    public void BuildId_GivenComponents_ReturnsUpperInvariantCompositeKey()
    {
        var id = ItemStockInventory.BuildId("wh1", "sku1", "925", "th");

        Assert.Equal("WH1:SKU1:925:TH", id);
    }

    [Fact(DisplayName = "ActivateExtension sets IsExtended to true")]
    public void ActivateExtension_Called_SetsIsExtendedTrue()
    {
        var aggregate = CreateAggregate(isExtended: false);

        aggregate.ActivateExtension();

        Assert.True(aggregate.IsExtended);
    }

    [Fact(DisplayName = "CreateDefault zero-initializes all quantity fields and stamps identity/ModifiedUtc")]
    public void CreateDefault_GivenComponents_ReturnsZeroInitializedAggregate()
    {
        var aggregate = ItemStockInventory.CreateDefault("WH1", "SKU1", "925", "TH", Now);

        Assert.Equal("WH1:SKU1:925:TH", aggregate.Id);
        Assert.Equal("WH1", aggregate.FulfilmentId);
        Assert.Equal("SKU1", aggregate.ItemCode);
        Assert.Equal("925", aggregate.Hallmark);
        Assert.Equal("TH", aggregate.CountryOfOrigin);
        Assert.Equal(Now, aggregate.ModifiedUtc);
        Assert.False(aggregate.IsExtended);
        Assert.Equal(0, aggregate.B2BAvailable);
        Assert.Equal(0, aggregate.B2CAvailable);
        Assert.Equal(0, aggregate.B2COriginal);
        Assert.Equal(0, aggregate.B2CExtended);
        Assert.Equal(0, aggregate.B2CAllocated);
        Assert.Equal(0, aggregate.B2BAllocated);
        Assert.Equal(0, aggregate.B2CPrepared);
        Assert.Equal(0, aggregate.B2BPrepared);
        Assert.Equal(0, aggregate.B2BUsedShare);
    }

    [Fact(DisplayName = "CalculateB2CExtended matches the doc's §3.4 worked example (B2BAVL=500, B2BAllocated=200, B2BUsedShare=40)")]
    public void CalculateB2CExtended_DocWorkedExample_ComputesExpectedValue()
    {
        var aggregate = CreateAggregate(b2bAvailable: 500, b2bAllocated: 200, b2bUsedShare: 40, b2bPrepared: 0);

        aggregate.CalculateB2CExtended();

        Assert.Equal(260, aggregate.B2CExtended);
    }

    [Fact(DisplayName = "CalculateB2CExtended clamps to zero when B2B commitments exceed B2BAvailable")]
    public void CalculateB2CExtended_CommitmentsExceedAvailable_ClampsToZero()
    {
        var aggregate = CreateAggregate(b2bAvailable: 10, b2bAllocated: 20, b2bUsedShare: 0, b2bPrepared: 0);

        aggregate.CalculateB2CExtended();

        Assert.Equal(0, aggregate.B2CExtended);
    }

    [Fact(DisplayName = "DoFulfilmentLevelB2CSegmentation adds a positive inbound quantity directly to B2CAvailable")]
    public void DoFulfilmentLevelB2CSegmentation_PositiveInboundQty_AddsToB2CAvailable()
    {
        var aggregate = CreateAggregate(b2cAvailable: 20, b2cAllocated: 0, b2cPrepared: 0);

        aggregate.DoFulfilmentLevelB2CSegmentation(5, Now);

        Assert.Equal(25, aggregate.B2CAvailable);
        Assert.Equal(Now, aggregate.ModifiedUtc);
    }

    [Fact(DisplayName = "DoFulfilmentLevelB2CSegmentation subtracts a negative inbound quantity that fits within the actual available amount")]
    public void DoFulfilmentLevelB2CSegmentation_NegativeInboundQtyWithinActualAvailable_SubtractsExactly()
    {
        var aggregate = CreateAggregate(b2cAvailable: 20, b2cAllocated: 5, b2cPrepared: 5);

        aggregate.DoFulfilmentLevelB2CSegmentation(-6, Now);

        Assert.Equal(14, aggregate.B2CAvailable);
    }

    [Fact(DisplayName = "DoFulfilmentLevelB2CSegmentation clamps B2CAvailable to zero on an oversell rather than rejecting")]
    public void DoFulfilmentLevelB2CSegmentation_NegativeInboundQtyOversells_ClampsToZero()
    {
        var aggregate = CreateAggregate(b2cAvailable: 20, b2cAllocated: 5, b2cPrepared: 5);

        aggregate.DoFulfilmentLevelB2CSegmentation(-25, Now);

        Assert.Equal(0, aggregate.B2CAvailable);
    }

    [Fact(DisplayName = "DoFulfilmentLevelSegmentation adds a positive inbound quantity to B2BAvailable and leaves B2CAvailable untouched")]
    public void DoFulfilmentLevelSegmentation_PositiveInboundQty_AddsToB2BAvailableOnly()
    {
        var aggregate = CreateAggregate(b2bAvailable: 20, b2bAllocated: 0, b2bUsedShare: 0, b2bPrepared: 0, b2cAvailable: 20);

        aggregate.DoFulfilmentLevelSegmentation(5, Now);

        Assert.Equal(25, aggregate.B2BAvailable);
        Assert.Equal(20, aggregate.B2CAvailable);
    }

    [Fact(DisplayName = "DoFulfilmentLevelSegmentation clamps B2BAvailable to zero on an oversell and leaves B2CAvailable untouched")]
    public void DoFulfilmentLevelSegmentation_NegativeInboundQtyOversells_ClampsB2BAvailableToZeroOnly()
    {
        var aggregate = CreateAggregate(b2bAvailable: 20, b2bAllocated: 5, b2bUsedShare: 0, b2bPrepared: 5, b2cAvailable: 20);

        aggregate.DoFulfilmentLevelSegmentation(-25, Now);

        Assert.Equal(0, aggregate.B2BAvailable);
        Assert.Equal(20, aggregate.B2CAvailable);
    }

    [Fact(DisplayName = "DoItemLevelExtension is a no-op when IsExtended is false")]
    public void DoItemLevelExtension_NotExtended_DoesNothing()
    {
        var aggregate = CreateAggregate(isExtended: false, b2cOriginal: 100, b2bAvailable: 50);

        aggregate.DoItemLevelExtension(8, 90, Now.AddHours(1));

        Assert.Equal(100, aggregate.B2COriginal);
        Assert.Equal(50, aggregate.B2BAvailable);
        Assert.Equal(Now, aggregate.ModifiedUtc);
    }

    [Fact(DisplayName = "DoItemLevelExtension adds a positive inbound quantity directly to B2COriginal when it fits within the ecom share")]
    public void DoItemLevelExtension_PositiveInboundQtyFitsWithinEcomShare_AddsToB2COriginal()
    {
        var aggregate = CreateAggregate(
            isExtended: true, b2cOriginal: 100, b2cAllocated: 10, b2cPrepared: 10, b2cExtended: 0);

        aggregate.DoItemLevelExtension(8, 90, Now);

        Assert.Equal(108, aggregate.B2COriginal);
        Assert.Equal(0, aggregate.B2CExtended);
        Assert.Equal(108, aggregate.B2CAvailable);
    }

    [Fact(DisplayName = "DoItemLevelExtension splits a positive inbound quantity across B2COriginal and B2BAvailable when it exceeds the ecom share")]
    public void DoItemLevelExtension_PositiveInboundQtyExceedsEcomShare_SplitsAcrossB2COriginalAndB2BAvailable()
    {
        var aggregate = CreateAggregate(
            isExtended: true, b2cOriginal: 100, b2cAllocated: 10, b2cPrepared: 10, b2cExtended: 0,
            b2bAvailable: 50, b2bAllocated: 10, b2bUsedShare: 0, b2bPrepared: 0);

        aggregate.DoItemLevelExtension(12, 85, Now);

        Assert.Equal(105, aggregate.B2COriginal);
        Assert.Equal(57, aggregate.B2BAvailable);
        Assert.Equal(47, aggregate.B2CExtended);
        Assert.Equal(152, aggregate.B2CAvailable);
    }

    [Fact(DisplayName = "DoItemLevelExtension subtracts a negative inbound quantity directly from B2BAvailable when fully absorbed")]
    public void DoItemLevelExtension_NegativeInboundQtyFullyAbsorbedByB2BAvailable_SubtractsFromB2BAvailable()
    {
        var aggregate = CreateAggregate(
            isExtended: true, b2cOriginal: 100, b2cExtended: 0,
            b2bAvailable: 50, b2bAllocated: 10, b2bUsedShare: 0, b2bPrepared: 0);

        aggregate.DoItemLevelExtension(-15, 0, Now);

        Assert.Equal(35, aggregate.B2BAvailable);
        Assert.Equal(100, aggregate.B2COriginal);
        Assert.Equal(25, aggregate.B2CExtended);
        Assert.Equal(125, aggregate.B2CAvailable);
    }

    [Fact(DisplayName = "DoItemLevelExtension splits a negative inbound quantity between B2COriginal and B2BAvailable when it exceeds the actual B2BAvailable")]
    public void DoItemLevelExtension_NegativeInboundQtyExceedsActualB2BAvailable_SplitsBetweenB2COriginalAndB2BAvailable()
    {
        var aggregate = CreateAggregate(
            isExtended: true, b2cOriginal: 100, b2cExtended: 0,
            b2bAvailable: 50, b2bAllocated: 10, b2bUsedShare: 0, b2bPrepared: 0);

        aggregate.DoItemLevelExtension(-45, 0, Now);

        Assert.Equal(95, aggregate.B2COriginal);
        Assert.Equal(10, aggregate.B2BAvailable);
        Assert.Equal(0, aggregate.B2CExtended);
        Assert.Equal(95, aggregate.B2CAvailable);
    }
}
