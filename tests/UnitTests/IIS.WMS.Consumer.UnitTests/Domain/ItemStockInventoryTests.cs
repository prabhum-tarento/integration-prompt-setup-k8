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
        int b2bAllocated = 10, int b2cAllocated = 10, bool isExtended = false, int b2bUsedShare = 0) =>
        ItemStockInventory.Rehydrate(
            id: "WH1:SKU1:925:TH",
            fulfilmentId: "WH1",
            itemCode: "SKU1",
            countryOfOrigin: "TH",
            hallmark: "925",
            b2bAvailable: 20,
            b2cAvailable: 20,
            b2cOriginal: 20,
            b2cExtended: 0,
            b2cAllocated: b2cAllocated,
            b2bAllocated: b2bAllocated,
            b2cPrepared: 0,
            b2bPrepared: 0,
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

    [Fact(DisplayName = "Unpick decrements B2BPrepared and raises ItemStockUnpicked")]
    public void Unpick_PreparedQuantityAvailable_DecrementsB2BPreparedAndRaisesItemStockUnpicked()
    {
        var aggregate = CreateAggregate(b2bAllocated: 10);
        aggregate.PickB2B(6, Now);

        aggregate.Unpick(4, Now);

        Assert.Equal(2, aggregate.B2BPrepared);
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
}
