using IIS.WMS.Consumer.Domain.Exceptions;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>
/// Tests for <see cref="MissingItemStockInventoryException"/> - the <c>MISSING_INVENTORY</c> business
/// rejection (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.1/§8).
/// </summary>
public class MissingItemStockInventoryExceptionTests
{
    [Fact(DisplayName = "Constructor populates Id, ItemCode, and a message identifying the missing record")]
    public void Constructor_IdAndItemCode_PopulatesPropertiesAndMessage()
    {
        var exception = new MissingItemStockInventoryException("WH1:SKU1:925:TH", "SKU1");

        Assert.Equal("WH1:SKU1:925:TH", exception.Id);
        Assert.Equal("SKU1", exception.ItemCode);
        Assert.Contains("WH1:SKU1:925:TH", exception.Message);
        Assert.Contains("SKU1", exception.Message);
    }

    [Fact(DisplayName = "MissingItemStockInventoryException is a DomainException")]
    public void MissingItemStockInventoryException_Always_IsDomainException()
    {
        var exception = new MissingItemStockInventoryException("WH1:SKU1:925:TH", "SKU1");

        Assert.IsAssignableFrom<DomainException>(exception);
    }
}
