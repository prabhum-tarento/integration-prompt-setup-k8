using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Domain.Events;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>
/// Directly exercises <see cref="AggregateRoot"/>'s own bookkeeping - <c>RaiseDomainEvent</c>,
/// <c>DomainEvents</c>, and <c>ClearDomainEvents</c> - through the concrete <see cref="InventoryEvent"/>
/// aggregate, since <see cref="AggregateRoot"/> itself is abstract and has no fields other test files
/// exercise (<c>InventoryEventTests</c> covers <see cref="InventoryEvent"/>'s own business rules, not
/// left untouched here; this file targets the base class behavior specifically).
/// </summary>
public class AggregateRootTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    private static InventoryEvent CreateAggregate() =>
        InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", initialQuantity: 10, Now);

    [Fact(DisplayName = "DomainEvents is empty for a newly created aggregate that raised none")]
    public void DomainEvents_NoEventsRaised_IsEmpty()
    {
        var aggregate = CreateAggregate();

        Assert.Empty(aggregate.DomainEvents);
    }

    [Fact(DisplayName = "RaiseDomainEvent appends to DomainEvents in the order raised")]
    public void RaiseDomainEvent_MultipleCalls_PreservesRaiseOrder()
    {
        var aggregate = CreateAggregate();

        aggregate.Reserve("reservation-1", 4, Now);
        aggregate.Allocate("reservation-1", Now);

        Assert.Collection(
            aggregate.DomainEvents,
            e => Assert.IsType<StockReserved>(e),
            e => Assert.IsType<StockAllocated>(e));
    }

    [Fact(DisplayName = "ClearDomainEvents empties DomainEvents after events were raised")]
    public void ClearDomainEvents_AfterEventsRaised_EmptiesDomainEvents()
    {
        var aggregate = CreateAggregate();
        aggregate.Reserve("reservation-1", 4, Now);
        Assert.NotEmpty(aggregate.DomainEvents);

        aggregate.ClearDomainEvents();

        Assert.Empty(aggregate.DomainEvents);
    }

    [Fact(DisplayName = "ClearDomainEvents on an aggregate with no raised events is a no-op")]
    public void ClearDomainEvents_NoEventsRaised_RemainsEmpty()
    {
        var aggregate = CreateAggregate();

        aggregate.ClearDomainEvents();

        Assert.Empty(aggregate.DomainEvents);
    }

    [Fact(DisplayName = "RaiseDomainEvent after a ClearDomainEvents call starts a fresh collection")]
    public void RaiseDomainEvent_AfterClear_CollectsOnlyNewEvents()
    {
        var aggregate = CreateAggregate();
        aggregate.Reserve("reservation-1", 4, Now);
        aggregate.ClearDomainEvents();

        aggregate.Adjust(20, "cycle count", Now);

        var raised = Assert.Single(aggregate.DomainEvents);
        Assert.IsType<StockAdjusted>(raised);
    }

    [Fact(DisplayName = "Id is assigned by the derived aggregate's factory method")]
    public void Id_AfterCreate_MatchesFactoryArgument()
    {
        var aggregate = CreateAggregate();

        Assert.Equal("WH1:SKU1", aggregate.Id);
    }
}
