using IIS.WMS.Consumer.Application.BulkInventoryImport.Dtos;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Construction/equality tests for the <see cref="ImportBulkInventoryItemRequest"/> record.</summary>
public class ImportBulkInventoryItemRequestTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "Constructor assigns every property")]
    public void Constructor_AllArguments_AssignsProperties()
    {
        var request = new ImportBulkInventoryItemRequest("WH1", "SKU1", 25, "WMS-Legacy", Now);

        Assert.Equal("WH1", request.WarehouseId);
        Assert.Equal("SKU1", request.Sku);
        Assert.Equal(25, request.Quantity);
        Assert.Equal("WMS-Legacy", request.SourceSystem);
        Assert.Equal(Now, request.SourceLastUpdatedUtc);
    }

    [Fact(DisplayName = "Two requests with the same values are equal - record value semantics")]
    public void Equals_SameValues_AreEqual()
    {
        var first = new ImportBulkInventoryItemRequest("WH1", "SKU1", 25, "WMS-Legacy", Now);
        var second = new ImportBulkInventoryItemRequest("WH1", "SKU1", 25, "WMS-Legacy", Now);

        Assert.Equal(first, second);
    }

    [Fact(DisplayName = "Two requests with a different quantity are not equal")]
    public void Equals_DifferentQuantity_AreNotEqual()
    {
        var first = new ImportBulkInventoryItemRequest("WH1", "SKU1", 25, "WMS-Legacy", Now);
        var second = new ImportBulkInventoryItemRequest("WH1", "SKU1", 30, "WMS-Legacy", Now);

        Assert.NotEqual(first, second);
    }
}
