using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>Correctness tests for <see cref="CosmosContainerNames.GetItemStockInventoryContainerName"/> - the per-fulfilment-code container resolver.</summary>
public class CosmosContainerNamesTests
{
    [Theory(DisplayName = "GetItemStockInventoryContainerName appends the fulfilment code's suffix to the base container name")]
    [InlineData("EDC", "ItemStockInventoryEDC")]
    [InlineData("TDC", "ItemStockInventoryTDC")]
    [InlineData("ADC", "ItemStockInventoryADC")]
    [InlineData("CAECOM", "ItemStockInventoryCAECOM")]
    [InlineData("BRZ3PL", "ItemStockInventoryBRZ3PL")]
    public void GetItemStockInventoryContainerName_AllowListedCode_ReturnsSuffixedContainerName(string fulfilmentCode, string expected)
    {
        var result = CosmosContainerNames.GetItemStockInventoryContainerName(fulfilmentCode);

        Assert.Equal(expected, result);
    }

    [Theory(DisplayName = "GetItemStockInventoryContainerName matches an allow-listed code regardless of casing")]
    [InlineData("edc")]
    [InlineData("Edc")]
    [InlineData("eDC")]
    public void GetItemStockInventoryContainerName_DifferentCasing_ResolvesSameContainerName(string fulfilmentCode)
    {
        var result = CosmosContainerNames.GetItemStockInventoryContainerName(fulfilmentCode);

        Assert.Equal("ItemStockInventoryEDC", result);
    }

    [Fact(DisplayName = "GetItemStockInventoryContainerName throws ArgumentException, listing the allow-list, for an unrecognized fulfilment code")]
    public void GetItemStockInventoryContainerName_UnrecognizedCode_ThrowsArgumentExceptionWithAllowList()
    {
        var exception = Assert.Throws<ArgumentException>(
            () => CosmosContainerNames.GetItemStockInventoryContainerName("WH1"));

        Assert.Contains("WH1", exception.Message);
        Assert.Contains("Edc", exception.Message);
        Assert.Contains("Tdc", exception.Message);
        Assert.Contains("Adc", exception.Message);
        Assert.Contains("CaEcom", exception.Message);
        Assert.Contains("Brz3Pl", exception.Message);
    }
}
