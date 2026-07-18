using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Events;
using IIS.WMS.Consumer.Domain.Exceptions;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>Business-rule tests for the <see cref="InventoryEvent"/> aggregate - reserve/allocate/release/adjust, oversell prevention, and idempotent redelivery.</summary>
public class InventoryEventTests
{
    private static readonly DateTime Now = new(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "Reserve succeeds when on-hand quantity covers the request")]
    public void Reserve_SufficientOnHand_DecrementsOnHandAndRaisesStockReserved()
    {
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, Now);

        aggregate.Reserve("reservation-1", 4, Now);

        Assert.Equal(6, aggregate.OnHandQuantity);
        Assert.Equal(4, aggregate.ActiveReservations["reservation-1"]);
        var raised = Assert.IsType<StockReserved>(Assert.Single(aggregate.DomainEvents));
        Assert.Equal(4, raised.Quantity);
    }

    [Fact(DisplayName = "Reserve throws when the request exceeds on-hand quantity")]
    public void Reserve_QuantityExceedsOnHand_ThrowsInsufficientStockException()
    {
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 3, Now);

        var exception = Assert.Throws<InsufficientStockException>(() => aggregate.Reserve("reservation-1", 4, Now));

        Assert.Equal(4, exception.Requested);
        Assert.Equal(3, exception.Available);
    }

    [Fact(DisplayName = "Reserve is a no-op when the same reservation id is redelivered")]
    public void Reserve_SameReservationIdRedelivered_DoesNotDecrementOnHandTwice()
    {
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, Now);

        aggregate.Reserve("reservation-1", 4, Now);
        aggregate.Reserve("reservation-1", 4, Now);

        Assert.Equal(6, aggregate.OnHandQuantity);
        Assert.Single(aggregate.DomainEvents);
    }

    [Fact(DisplayName = "Allocate removes the reservation without decrementing on-hand again")]
    public void Allocate_ExistingReservation_RemovesReservationAndRaisesStockAllocated()
    {
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, Now);
        aggregate.Reserve("reservation-1", 4, Now);

        aggregate.Allocate("reservation-1", Now);

        Assert.Equal(6, aggregate.OnHandQuantity);
        Assert.Empty(aggregate.ActiveReservations);
        Assert.Contains(aggregate.DomainEvents, e => e is StockAllocated);
    }

    [Fact(DisplayName = "Allocate throws when there is no matching reservation")]
    public void Allocate_NoMatchingReservation_ThrowsInvalidOperationException()
    {
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, Now);

        Assert.Throws<InvalidOperationException>(() => aggregate.Allocate("unknown-reservation", Now));
    }

    [Fact(DisplayName = "ReleaseReservation returns the quantity to on-hand")]
    public void ReleaseReservation_ExistingReservation_RestoresOnHandQuantity()
    {
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, Now);
        aggregate.Reserve("reservation-1", 4, Now);

        aggregate.ReleaseReservation("reservation-1", Now);

        Assert.Equal(10, aggregate.OnHandQuantity);
        Assert.Empty(aggregate.ActiveReservations);
    }

    [Fact(DisplayName = "ReleaseReservation is idempotent when the reservation is already gone")]
    public void ReleaseReservation_AlreadyReleased_DoesNotDoubleCreditOnHand()
    {
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, Now);
        aggregate.Reserve("reservation-1", 4, Now);
        aggregate.ReleaseReservation("reservation-1", Now);

        aggregate.ReleaseReservation("reservation-1", Now);

        Assert.Equal(10, aggregate.OnHandQuantity);
    }

    [Fact(DisplayName = "Adjust sets on-hand directly and raises StockAdjusted")]
    public void Adjust_NewQuantity_SetsOnHandAndRaisesStockAdjusted()
    {
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, Now);

        aggregate.Adjust(7, "cycle count", Now);

        Assert.Equal(7, aggregate.OnHandQuantity);
        var raised = Assert.IsType<StockAdjusted>(Assert.Single(aggregate.DomainEvents));
        Assert.Equal(10, raised.PreviousQuantity);
        Assert.Equal(7, raised.NewQuantity);
    }

    [Fact(DisplayName = "Rehydrate restores on-hand quantity and active reservations without raising domain events")]
    public void Rehydrate_PersistedState_RestoresStateWithoutDomainEvents()
    {
        var reservations = new Dictionary<string, int> { ["reservation-1"] = 2 };

        var aggregate = InventoryEvent.Rehydrate("WH1:SKU1", "WH1", "SKU1", 8, Now, Now, reservations);

        Assert.Equal(8, aggregate.OnHandQuantity);
        Assert.Equal(2, aggregate.ActiveReservations["reservation-1"]);
        Assert.Empty(aggregate.DomainEvents);
    }

    [Fact(DisplayName = "Category combines warehouse id and SKU as the Cosmos partition key")]
    public void Category_ReturnsWarehouseIdAndSkuJoinedByColon()
    {
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, Now);

        Assert.Equal("WH1:SKU1", aggregate.Category);
    }
}
