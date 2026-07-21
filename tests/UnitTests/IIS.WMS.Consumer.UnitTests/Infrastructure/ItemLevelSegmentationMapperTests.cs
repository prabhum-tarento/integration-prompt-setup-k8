using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

public class ItemLevelSegmentationMapperGeneralTests
{
    [Fact(DisplayName = "ToDocument_WithValidAggregate_GeneratesCorrectId")]
    public void ToDocument_WithValidAggregate_GeneratesCorrectId()
    {
        // Arrange
        var aggregate = new ItemLevelSegmentation
        {
            ItemCode = "SKU001",
            CountryOfOriginCode = "US",
            FulfilmentCode = "EDC",
            HallmarkCode = "TYPE_A",
            IsActive = true,
            ETag = "tag1"
        };

        // Act
        var document = ItemLevelSegmentationMapper.ToDocument(aggregate);

        // Assert
        Assert.Equal("SKU001_US", document.Id);
    }

    [Fact(DisplayName = "ToDocument_WithValidAggregate_GeneratesCorrectCategory")]
    public void ToDocument_WithValidAggregate_GeneratesCorrectCategory()
    {
        // Arrange
        var aggregate = new ItemLevelSegmentation
        {
            ItemCode = "SKU001",
            CountryOfOriginCode = "US",
            FulfilmentCode = "EDC",
            HallmarkCode = "TYPE_A",
            IsActive = true,
            ETag = "tag1"
        };

        // Act
        var document = ItemLevelSegmentationMapper.ToDocument(aggregate);

        // Assert
        Assert.Equal("SEG_ITEM_EDC_TYPE_A", document.Category);
    }

    [Fact(DisplayName = "ToDocument_PreservesETag_ForOptimisticConcurrencyControl")]
    public void ToDocument_PreservesETag_ForOptimisticConcurrencyControl()
    {
        // Arrange
        var etag = "\"0x8D8F1A2B3C4D5E6F\"";
        var aggregate = new ItemLevelSegmentation
        {
            ItemCode = "SKU001",
            CountryOfOriginCode = "US",
            FulfilmentCode = "EDC",
            HallmarkCode = "TYPE_A",
            IsActive = true,
            ETag = etag
        };

        // Act
        var document = ItemLevelSegmentationMapper.ToDocument(aggregate);

        // Assert
        Assert.Equal(etag, document.ETag);
    }

    [Fact(DisplayName = "ToDomain_WithValidDocument_RehydratesAllProperties")]
    public void ToDomain_WithValidDocument_RehydratesAllProperties()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var document = new ItemLevelSegmentationDocument
        {
            Id = "SKU001_US",
            Category = "SEG_ITEM_EDC_TYPE_A",
            FulfilmentCode = "EDC",
            HallmarkCode = "TYPE_A",
            ItemCode = "SKU001",
            CountryOfOriginCode = "US",
            EcomShare = 40,
            StoreLeveragePercentage = 30.5m,
            ThresholdPercentage = 15.0m,
            IsOMNI = true,
            CurrentOmniStock = 500,
            CurrentEcomStock = 300,
            StoreShare = 40,
            EcomStatus = 1,
            IsExtended = true,
            Notes = "Sample notes",
            InTransit = 20,
            LastModified = now,
            IsActive = true,
            ETag = "tag1"
        };

        // Act
        var aggregate = ItemLevelSegmentationMapper.ToDomain(document);

        // Assert
        Assert.Equal(document.FulfilmentCode, aggregate.FulfilmentCode);
        Assert.Equal(document.HallmarkCode, aggregate.HallmarkCode);
        Assert.Equal(document.ItemCode, aggregate.ItemCode);
        Assert.Equal(document.CountryOfOriginCode, aggregate.CountryOfOriginCode);
        Assert.Equal(document.EcomShare, aggregate.EcomShare);
        Assert.Equal(document.StoreLeveragePercentage, aggregate.StoreLeveragePercentage);
        Assert.Equal(document.ThresholdPercentage, aggregate.ThresholdPercentage);
        Assert.Equal(document.IsOMNI, aggregate.IsOMNI);
        Assert.Equal(document.CurrentOmniStock, aggregate.CurrentOmniStock);
        Assert.Equal(document.CurrentEcomStock, aggregate.CurrentEcomStock);
        Assert.Equal(document.StoreShare, aggregate.StoreShare);
        Assert.Equal(document.EcomStatus, aggregate.EcomStatus);
        Assert.Equal(document.IsExtended, aggregate.IsExtended);
        Assert.Equal(document.Notes, aggregate.Notes);
        Assert.Equal(document.InTransit, aggregate.InTransit);
        Assert.Equal(document.LastModified, aggregate.LastModified);
        Assert.Equal(document.IsActive, aggregate.IsActive);
        Assert.Equal(document.ETag, aggregate.ETag);
    }

    [Fact(DisplayName = "ToDomain_WithNullableProperties_ToleratesAbsentProperties")]
    public void ToDomain_WithNullableProperties_ToleratesAbsentProperties()
    {
        // Arrange - document with null/default nullable properties, per cosmos-db.instructions.md §5
        var document = new ItemLevelSegmentationDocument
        {
            Id = "SKU002_CA",
            Category = "SEG_ITEM_TDC_TYPE_B",
            FulfilmentCode = "TDC",
            HallmarkCode = "TYPE_B",
            ItemCode = "SKU002",
            CountryOfOriginCode = "CA",
            EcomShare = null,
            StoreLeveragePercentage = null,
            ThresholdPercentage = null,
            IsOMNI = false,
            CurrentOmniStock = null,
            CurrentEcomStock = null,
            StoreShare = null,
            EcomStatus = null,
            IsExtended = false,
            Notes = null!,
            InTransit = null,
            LastModified = null,
            IsActive = false,
            ETag = null
        };

        // Act
        var aggregate = ItemLevelSegmentationMapper.ToDomain(document);

        // Assert
        Assert.Null(aggregate.EcomShare);
        Assert.Null(aggregate.StoreLeveragePercentage);
        Assert.Null(aggregate.ThresholdPercentage);
        Assert.Null(aggregate.CurrentOmniStock);
        Assert.Null(aggregate.CurrentEcomStock);
        Assert.Null(aggregate.StoreShare);
        Assert.Null(aggregate.EcomStatus);
        Assert.Null(aggregate.InTransit);
        Assert.Null(aggregate.LastModified);
        Assert.Null(aggregate.ETag);
    }

    [Theory(DisplayName = "ToDocument_DifferentFulfilments_GenerateUniqueCategoryPerFulfilment")]
    [InlineData("EDC", "TDC", "TYPE_A")]
    [InlineData("ADC", "CAECOM", "TYPE_B")]
    [InlineData("BRZ3PL", "EDC", "TYPE_C")]
    public void ToDocument_DifferentFulfilments_GenerateUniqueCategoryPerFulfilment(
        string fulfilment1, string fulfilment2, string hallmark)
    {
        // Arrange
        var agg1 = new ItemLevelSegmentation
        {
            ItemCode = "SKU",
            CountryOfOriginCode = "US",
            FulfilmentCode = fulfilment1,
            HallmarkCode = hallmark,
            IsActive = true,
            ETag = "tag"
        };

        var agg2 = new ItemLevelSegmentation
        {
            ItemCode = "SKU",
            CountryOfOriginCode = "US",
            FulfilmentCode = fulfilment2,
            HallmarkCode = hallmark,
            IsActive = true,
            ETag = "tag"
        };

        // Act
        var doc1 = ItemLevelSegmentationMapper.ToDocument(agg1);
        var doc2 = ItemLevelSegmentationMapper.ToDocument(agg2);

        // Assert - same item/coo but different fulfilments produce different categories
        Assert.Equal(doc1.Id, doc2.Id);
        Assert.NotEqual(doc1.Category, doc2.Category);
    }

    [Fact(DisplayName = "ToDocument_ZeroAndNegativeNumbers_PreserveNumericValues")]
    public void ToDocument_ZeroAndNegativeNumbers_PreserveNumericValues()
    {
        // Arrange
        var aggregate = new ItemLevelSegmentation
        {
            ItemCode = "SKU",
            CountryOfOriginCode = "US",
            FulfilmentCode = "EDC",
            HallmarkCode = "TYPE",
            EcomShare = 0,
            CurrentOmniStock = 0,
            CurrentEcomStock = -5, // negative stock edge case
            StoreShare = 0,
            InTransit = 0,
            IsActive = true,
            ETag = "tag"
        };

        // Act
        var document = ItemLevelSegmentationMapper.ToDocument(aggregate);

        // Assert
        Assert.Equal(0, document.EcomShare);
        Assert.Equal(0, document.CurrentOmniStock);
        Assert.Equal(-5, document.CurrentEcomStock);
        Assert.Equal(0, document.StoreShare);
        Assert.Equal(0, document.InTransit);
    }
}
