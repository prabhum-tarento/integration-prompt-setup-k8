using IIS.WMS.Consumer.Domain.Exceptions;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>
/// Tests for <see cref="InvalidItemStockInventoryQtyException"/> - the <c>INVALID_QUANTITY</c> business
/// rejection (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.1/§3.3/§8).
/// </summary>
public class InvalidItemStockInventoryQtyExceptionTests
{
    [Fact(DisplayName = "Constructor populates Id, ItemCode, Requested, ResultingValue, and a summarizing message")]
    public void Constructor_AllFields_PopulatesPropertiesAndMessage()
    {
        var exception = new InvalidItemStockInventoryQtyException("WH1:SKU1:925:TH", "SKU1", 10, -2);

        Assert.Equal("WH1:SKU1:925:TH", exception.Id);
        Assert.Equal("SKU1", exception.ItemCode);
        Assert.Equal(10, exception.Requested);
        Assert.Equal(-2, exception.ResultingValue);
        Assert.Contains("10", exception.Message);
        Assert.Contains("SKU1", exception.Message);
        Assert.Contains("WH1:SKU1:925:TH", exception.Message);
    }

    [Fact(DisplayName = "InvalidItemStockInventoryQtyException is a DomainException")]
    public void InvalidItemStockInventoryQtyException_Always_IsDomainException()
    {
        var exception = new InvalidItemStockInventoryQtyException("WH1:SKU1:925:TH", "SKU1", 10, -2);

        Assert.IsAssignableFrom<DomainException>(exception);
    }
}
