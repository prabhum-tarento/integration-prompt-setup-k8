using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

public class CountryMapperTests
{
    [Fact(DisplayName = "ToDocument_WithValidAggregate_GeneratesCodeKeyedId")]
    public void ToDocument_WithValidAggregate_GeneratesCodeKeyedId()
    {
        // Arrange
        var aggregate = new CountryMaster
        {
            Code = "TH",
            Name = "Thailand",
            RegionCode = "APAC",
            IsAX12Market = true,
            IsActive = true,
        };

        // Act
        var document = CountryMapper.ToDocument(aggregate);

        // Assert
        Assert.Equal("TH", document.Id);
    }

    [Fact(DisplayName = "ToDocument_WithValidAggregate_GeneratesCodeKeyedCategory")]
    public void ToDocument_WithValidAggregate_GeneratesCodeKeyedCategory()
    {
        // Arrange
        var aggregate = new CountryMaster
        {
            Code = "TH",
            Name = "Thailand",
            RegionCode = "APAC",
            IsAX12Market = true,
            IsActive = true,
        };

        // Act
        var document = CountryMapper.ToDocument(aggregate);

        // Assert
        Assert.Equal("Country_TH", document.Category);
    }

    [Fact(DisplayName = "ToDocument_WithValidAggregate_PreservesNameRegionCodeAndAX12Flag")]
    public void ToDocument_WithValidAggregate_PreservesNameRegionCodeAndAX12Flag()
    {
        // Arrange
        var aggregate = new CountryMaster
        {
            Code = "TH",
            Name = "Thailand",
            RegionCode = "APAC",
            IsAX12Market = true,
            IsActive = true,
        };

        // Act
        var document = CountryMapper.ToDocument(aggregate);

        // Assert
        Assert.Equal("Thailand", document.Name);
        Assert.Equal("APAC", document.RegionCode);
        Assert.Equal("TH", document.CountryCode);
        Assert.True(document.IsAX12Market);
    }

    [Fact(DisplayName = "ToDomain_WithValidDocument_RehydratesCodeNameRegionAndAX12Flag")]
    public void ToDomain_WithValidDocument_RehydratesCodeNameRegionAndAX12Flag()
    {
        // Arrange
        var document = new CountryDocument
        {
            Id = "TH",
            Category = "Country_TH",
            Name = "Thailand",
            RegionCode = "APAC",
            CountryCode = "TH",
            IsAX12Market = true,
        };

        // Act
        var aggregate = CountryMapper.ToDomain(document);

        // Assert
        Assert.Equal(document.CountryCode, aggregate.Code);
        Assert.Equal(document.Name, aggregate.Name);
        Assert.Equal(document.RegionCode, aggregate.RegionCode);
        Assert.Equal(document.IsAX12Market, aggregate.IsAX12Market);
    }

    [Fact(DisplayName = "ToDomain_WithAnyDocument_AlwaysReportsIsActiveTrue")]
    public void ToDomain_WithAnyDocument_AlwaysReportsIsActiveTrue()
    {
        // Arrange - CountryDocument has no IsActive column today (TODO(ai) in CountryMapper.ToDomain)
        var document = new CountryDocument
        {
            Id = "TH",
            Category = "Country_TH",
            Name = "Thailand",
            RegionCode = "APAC",
            CountryCode = "TH",
            IsAX12Market = false,
        };

        // Act
        var aggregate = CountryMapper.ToDomain(document);

        // Assert
        Assert.True(aggregate.IsActive);
    }

    [Theory(DisplayName = "ToDocument_DifferentCodes_GenerateUniqueCategoryPerCode")]
    [InlineData("TH", "MY")]
    [InlineData("US", "CA")]
    public void ToDocument_DifferentCodes_GenerateUniqueCategoryPerCode(string code1, string code2)
    {
        // Arrange
        var agg1 = new CountryMaster { Code = code1, Name = "Country1", RegionCode = "APAC", IsActive = true };
        var agg2 = new CountryMaster { Code = code2, Name = "Country2", RegionCode = "APAC", IsActive = true };

        // Act
        var doc1 = CountryMapper.ToDocument(agg1);
        var doc2 = CountryMapper.ToDocument(agg2);

        // Assert - same region but different codes produce different categories (code-keyed, not region-keyed)
        Assert.NotEqual(doc1.Category, doc2.Category);
        Assert.NotEqual(doc1.Id, doc2.Id);
    }
}
