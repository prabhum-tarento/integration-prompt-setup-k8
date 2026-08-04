using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Events;
using IIS.WMS.Consumer.Domain.Exceptions;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>
/// Business-rule tests for the <see cref="ItemStockIntransit"/> aggregate - composite-id construction
/// and the never-negative quantity invariant (docs/events/inventory.InternalHallmarkingStatusChanged.md §5.2/§6).
/// </summary>
public class ItemStockIntransitTests
{
    private static readonly DateTime Now = new(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "BuildId concatenates the composite key parts, upper-invariant")]
    public void BuildId_VariousInputs_ReturnsUppercaseColonSeparatedComposite()
    {
        var id = ItemStockIntransit.BuildId("sku1", "925", "th", "internalhallmarking", "wh1", "allocated");

        Assert.Equal("SKU1:925:TH:INTERNALHALLMARKING:WH1:ALLOCATED", id);
    }

    [Fact(DisplayName = "CreateDefault builds a zero-quantity aggregate with the composite id and given ModifiedUtc")]
    public void CreateDefault_Always_BuildsZeroQuantityAggregate()
    {
        var aggregate = ItemStockIntransit.CreateDefault("SKU1", "925", "TH", "INTERNALHALLMARKING", "WH1", "ALLOCATED", Now);

        Assert.Equal("SKU1:925:TH:INTERNALHALLMARKING:WH1:ALLOCATED", aggregate.Id);
        Assert.Equal(aggregate.Id, aggregate.Category);
        Assert.Equal("SKU1", aggregate.ItemCode);
        Assert.Equal("925", aggregate.HallmarkCode);
        Assert.Equal("TH", aggregate.CountryOfOriginCode);
        Assert.Equal("INTERNALHALLMARKING", aggregate.OrderType);
        Assert.Equal("WH1", aggregate.FulfilmentCode);
        Assert.Equal("ALLOCATED", aggregate.Status);
        Assert.Equal(0, aggregate.Quantity);
        Assert.Equal(Now, aggregate.ModifiedUtc);
        Assert.Empty(aggregate.DomainEvents);
    }

    [Fact(DisplayName = "Rehydrate restores every property from persisted state without raising domain events")]
    public void Rehydrate_PersistedState_RestoresAllPropertiesWithoutRaisingEvents()
    {
        var aggregate = ItemStockIntransit.Rehydrate(
            id: "SKU1:925:TH:INTERNALHALLMARKING:WH1:PICKED",
            itemCode: "SKU1",
            hallmarkCode: "925",
            countryOfOriginCode: "TH",
            orderType: "INTERNALHALLMARKING",
            fulfilmentCode: "WH1",
            status: "PICKED",
            quantity: 15,
            modifiedUtc: Now);

        Assert.Equal("SKU1:925:TH:INTERNALHALLMARKING:WH1:PICKED", aggregate.Id);
        Assert.Equal(15, aggregate.Quantity);
        Assert.Equal("PICKED", aggregate.Status);
        Assert.Equal(Now, aggregate.ModifiedUtc);
        Assert.Empty(aggregate.DomainEvents);
    }

    [Fact(DisplayName = "IncreaseQuantity adds to Quantity, updates ModifiedUtc, and raises a positive-delta domain event")]
    public void IncreaseQuantity_PositiveQuantity_AddsAndRaisesPositiveDeltaEvent()
    {
        var aggregate = ItemStockIntransit.CreateDefault("SKU1", "925", "TH", "INTERNALHALLMARKING", "WH1", "ALLOCATED", Now);
        var later = Now.AddMinutes(5);

        aggregate.IncreaseQuantity(10, later);

        Assert.Equal(10, aggregate.Quantity);
        Assert.Equal(later, aggregate.ModifiedUtc);
        var raised = Assert.IsType<ItemStockIntransitQuantityChanged>(Assert.Single(aggregate.DomainEvents));
        Assert.Equal(aggregate.Id, raised.ItemStockIntransitId);
        Assert.Equal("SKU1", raised.ItemCode);
        Assert.Equal("ALLOCATED", raised.Status);
        Assert.Equal(10, raised.SignedQuantity);
    }

    [Theory(DisplayName = "IncreaseQuantity throws ArgumentOutOfRangeException for zero or negative quantities")]
    [InlineData(0)]
    [InlineData(-1)]
    public void IncreaseQuantity_ZeroOrNegativeQuantity_ThrowsArgumentOutOfRangeException(int quantity)
    {
        var aggregate = ItemStockIntransit.CreateDefault("SKU1", "925", "TH", "INTERNALHALLMARKING", "WH1", "ALLOCATED", Now);

        Assert.Throws<ArgumentOutOfRangeException>(() => aggregate.IncreaseQuantity(quantity, Now));
        Assert.Empty(aggregate.DomainEvents);
    }

    [Fact(DisplayName = "DecreaseQuantity subtracts from Quantity, updates ModifiedUtc, and raises a negative-delta domain event")]
    public void DecreaseQuantity_SufficientQuantity_SubtractsAndRaisesNegativeDeltaEvent()
    {
        var aggregate = ItemStockIntransit.Rehydrate(
            "SKU1:925:TH:INTERNALHALLMARKING:WH1:ALLOCATED", "SKU1", "925", "TH", "INTERNALHALLMARKING", "WH1", "ALLOCATED", 10, Now);
        var later = Now.AddMinutes(5);

        aggregate.DecreaseQuantity(4, later);

        Assert.Equal(6, aggregate.Quantity);
        Assert.Equal(later, aggregate.ModifiedUtc);
        var raised = Assert.IsType<ItemStockIntransitQuantityChanged>(Assert.Single(aggregate.DomainEvents));
        Assert.Equal(-4, raised.SignedQuantity);
    }

    [Theory(DisplayName = "DecreaseQuantity throws ArgumentOutOfRangeException for zero or negative quantities")]
    [InlineData(0)]
    [InlineData(-1)]
    public void DecreaseQuantity_ZeroOrNegativeQuantity_ThrowsArgumentOutOfRangeException(int quantity)
    {
        var aggregate = ItemStockIntransit.Rehydrate(
            "SKU1:925:TH:INTERNALHALLMARKING:WH1:ALLOCATED", "SKU1", "925", "TH", "INTERNALHALLMARKING", "WH1", "ALLOCATED", 10, Now);

        Assert.Throws<ArgumentOutOfRangeException>(() => aggregate.DecreaseQuantity(quantity, Now));
        Assert.Empty(aggregate.DomainEvents);
    }

    [Fact(DisplayName = "DecreaseQuantity throws InvalidItemStockInventoryQtyException rather than going negative")]
    public void DecreaseQuantity_QuantityExceedsOnHand_ThrowsInvalidItemStockInventoryQtyExceptionAndDoesNotMutate()
    {
        var aggregate = ItemStockIntransit.Rehydrate(
            "SKU1:925:TH:INTERNALHALLMARKING:WH1:ALLOCATED", "SKU1", "925", "TH", "INTERNALHALLMARKING", "WH1", "ALLOCATED", 5, Now);

        var exception = Assert.Throws<InvalidItemStockInventoryQtyException>(() => aggregate.DecreaseQuantity(6, Now));

        Assert.Equal(aggregate.Id, exception.Id);
        Assert.Equal("SKU1", exception.ItemCode);
        Assert.Equal(6, exception.Requested);
        Assert.Equal(5, aggregate.Quantity);
        Assert.Empty(aggregate.DomainEvents);
    }
}
