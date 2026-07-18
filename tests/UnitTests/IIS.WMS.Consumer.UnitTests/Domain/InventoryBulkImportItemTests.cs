using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>Business-rule tests for the <see cref="InventoryBulkImportItem"/> aggregate - required-field validation, the deterministic id/category, and the non-negative quantity guard.</summary>
public class InventoryBulkImportItemTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "Create derives Id and Category from warehouse and SKU and sets every property")]
    public void Create_ValidInputs_DerivesIdAndCategoryAndSetsProperties()
    {
        var item = InventoryBulkImportItem.Create("WH1", "SKU1", quantity: 25, sourceSystem: "WMS-Legacy", Now);

        Assert.Equal("WH1:SKU1", item.Id);
        Assert.Equal("WH1:SKU1", item.Category);
        Assert.Equal("WH1", item.WarehouseId);
        Assert.Equal("SKU1", item.Sku);
        Assert.Equal(25, item.Quantity);
        Assert.Equal("WMS-Legacy", item.SourceSystem);
        Assert.Equal(Now, item.SourceLastUpdatedUtc);
        Assert.Null(item.ETag);
        Assert.Empty(item.DomainEvents);
    }

    [Fact(DisplayName = "Create allows zero quantity - the non-negative boundary, not just positive quantities")]
    public void Create_ZeroQuantity_Succeeds()
    {
        var item = InventoryBulkImportItem.Create("WH1", "SKU1", quantity: 0, "WMS-Legacy", Now);

        Assert.Equal(0, item.Quantity);
    }

    [Fact(DisplayName = "Create throws when quantity is negative - the oversell-prevention guard for bulk-imported on-hand figures")]
    public void Create_NegativeQuantity_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => InventoryBulkImportItem.Create("WH1", "SKU1", -1, "WMS-Legacy", Now));
    }

    [Theory(DisplayName = "Create throws when warehouseId is null, empty, or whitespace")]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Create_BlankWarehouseId_ThrowsArgumentException(string? warehouseId)
    {
        Assert.ThrowsAny<ArgumentException>(() => InventoryBulkImportItem.Create(warehouseId!, "SKU1", 10, "WMS-Legacy", Now));
    }

    [Theory(DisplayName = "Create throws when sku is null, empty, or whitespace")]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Create_BlankSku_ThrowsArgumentException(string? sku)
    {
        Assert.ThrowsAny<ArgumentException>(() => InventoryBulkImportItem.Create("WH1", sku!, 10, "WMS-Legacy", Now));
    }

    [Theory(DisplayName = "Create throws when sourceSystem is null, empty, or whitespace")]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Create_BlankSourceSystem_ThrowsArgumentException(string? sourceSystem)
    {
        Assert.ThrowsAny<ArgumentException>(() => InventoryBulkImportItem.Create("WH1", "SKU1", 10, sourceSystem!, Now));
    }

    [Fact(DisplayName = "Rehydrate assigns every property verbatim without raising domain events")]
    public void Rehydrate_PersistedState_AssignsPropertiesVerbatimWithoutDomainEvents()
    {
        var item = InventoryBulkImportItem.Rehydrate("WH1:SKU1", "WH1", "SKU1", 40, "WMS-Legacy", Now);

        Assert.Equal("WH1:SKU1", item.Id);
        Assert.Equal("WH1:SKU1", item.Category);
        Assert.Equal(40, item.Quantity);
        Assert.Equal("WMS-Legacy", item.SourceSystem);
        Assert.Equal(Now, item.SourceLastUpdatedUtc);
        Assert.Empty(item.DomainEvents);
    }

    [Fact(DisplayName = "ETag can be set and read back - the repository's optimistic-concurrency token slot")]
    public void ETag_SetAfterCreate_ReturnsAssignedValue()
    {
        var item = InventoryBulkImportItem.Create("WH1", "SKU1", 10, "WMS-Legacy", Now);

        item.ETag = "\"etag-1\"";

        Assert.Equal("\"etag-1\"", item.ETag);
    }
}
