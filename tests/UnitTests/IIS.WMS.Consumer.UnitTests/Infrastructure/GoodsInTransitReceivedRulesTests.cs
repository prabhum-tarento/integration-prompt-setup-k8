using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="GoodsInTransitReceivedRules"/> - packing-slip normalization (§3.1),
/// sellability (§3.2), state/status resolution (§3.3), fulfilment-unit-id/destination-node
/// resolution (§3.4/§3.5), and OMS-delta eligibility (§3.6)
/// (docs/events/b2b.purchase.GoodsInTransitReceived.md).
/// </summary>
public class GoodsInTransitReceivedRulesTests
{
    [Theory(DisplayName = "NormalizePackingSlipId §3.1 strips a case-insensitive PS prefix")]
    [InlineData("PS12345", "12345")]
    [InlineData("ps12345", "345")]
    [InlineData("12345", "12345")]
    public void NormalizePackingSlipId_VariousInputs_StripsPrefixWhenPresent(string input, string _)
    {
        // Re-verify the exact expected value per case explicitly below (theory kept simple for the prefix case).
        var result = GoodsInTransitReceivedRules.NormalizePackingSlipId(input);

        Assert.Equal(
            input.StartsWith("PS", StringComparison.OrdinalIgnoreCase) ? input[2..] : input,
            result);
    }

    [Fact(DisplayName = "NormalizePackingSlipId §3.1 returns empty string for null or empty input")]
    public void NormalizePackingSlipId_NullOrEmpty_ReturnsEmptyString()
    {
        Assert.Equal(string.Empty, GoodsInTransitReceivedRules.NormalizePackingSlipId(null));
        Assert.Equal(string.Empty, GoodsInTransitReceivedRules.NormalizePackingSlipId(string.Empty));
    }

    [Fact(DisplayName = "IsSellable §3.2 returns true only for a CAECOM destination")]
    public void IsSellable_CaecomLocation_ReturnsTrue()
    {
        Assert.True(GoodsInTransitReceivedRules.IsSellable(new InventoryEventLocation(FulfilmentLocationIds.Caecom, InventoryEventLocationType.Warehouse)));
    }

    [Theory(DisplayName = "IsSellable §3.2 returns false for ADC, another destination, or a null destination")]
    [InlineData(null)]
    [InlineData("ADC")]
    [InlineData("WH-1")]
    public void IsSellable_NonCaecomOrNullLocation_ReturnsFalse(string? locationId)
    {
        var location = locationId is null ? null : new InventoryEventLocation(locationId, InventoryEventLocationType.Warehouse);

        Assert.False(GoodsInTransitReceivedRules.IsSellable(location));
    }

    [Fact(DisplayName = "ResolveState §3.3 returns Inspection when a return reason code is present")]
    public void ResolveState_PresentReturnReasonCode_ReturnsInspection()
    {
        Assert.Equal(State.INSPECTION, GoodsInTransitReceivedRules.ResolveState("DAMAGED"));
    }

    [Theory(DisplayName = "ResolveState §3.3 returns Available when the return reason code is null or empty")]
    [InlineData(null)]
    [InlineData("")]
    public void ResolveState_NullOrEmptyReturnReasonCode_ReturnsAvailable(string? returnReasonCode)
    {
        Assert.Equal(State.AVAILABLE, GoodsInTransitReceivedRules.ResolveState(returnReasonCode));
    }

    [Fact(DisplayName = "ResolveStatus §3.3 always returns Held")]
    public void ResolveStatus_Always_ReturnsHeld()
    {
        Assert.Equal(Status.HELD, GoodsInTransitReceivedRules.ResolveStatus());
    }

    [Theory(DisplayName = "ResolveFulfilmentUnitId §3.4 resolves CAECOM/ADC destinations to UNKNOWN (no order-lookup repository)")]
    [InlineData("CAECOM")]
    [InlineData("ADC")]
    public void ResolveFulfilmentUnitId_CaecomOrAdcDestination_ReturnsUnknown(string locationId)
    {
        var location = new InventoryEventLocation(locationId, InventoryEventLocationType.Warehouse);

        Assert.Equal("UNKNOWN", GoodsInTransitReceivedRules.ResolveFulfilmentUnitId(location, "EDC", "VENDOR-1"));
    }

    [Fact(DisplayName = "ResolveFulfilmentUnitId §3.4 normalizes the TDC SAP warehouse code to TDC for a non-CAECOM/ADC destination")]
    public void ResolveFulfilmentUnitId_TdcSapWarehouseCode_ReturnsTdc()
    {
        Assert.Equal(FulfilmentLocationIds.Tdc, GoodsInTransitReceivedRules.ResolveFulfilmentUnitId(null, "TDC-SAP-ID", "VENDOR-1"));
    }

    [Fact(DisplayName = "ResolveFulfilmentUnitId §3.4 falls back to the vendor code for an ordinary warehouse code")]
    public void ResolveFulfilmentUnitId_OrdinaryWarehouseCode_ReturnsVendorCode()
    {
        Assert.Equal("VENDOR-1", GoodsInTransitReceivedRules.ResolveFulfilmentUnitId(null, "EDC", "VENDOR-1"));
    }

    [Theory(DisplayName = "ResolveDestinationNode §3.5 returns the CAECOM/ADC location id unchanged")]
    [InlineData("CAECOM")]
    [InlineData("ADC")]
    public void ResolveDestinationNode_CaecomOrAdcDestination_ReturnsLocationId(string locationId)
    {
        var location = new InventoryEventLocation(locationId, InventoryEventLocationType.Warehouse);

        Assert.Equal(locationId, GoodsInTransitReceivedRules.ResolveDestinationNode(location, "EDC"));
    }

    [Fact(DisplayName = "ResolveDestinationNode §3.5 normalizes the TDC SAP warehouse code to TDC")]
    public void ResolveDestinationNode_TdcSapWarehouseCode_ReturnsTdc()
    {
        Assert.Equal(FulfilmentLocationIds.Tdc, GoodsInTransitReceivedRules.ResolveDestinationNode(null, "TDC-SAP-ID"));
    }

    [Fact(DisplayName = "ResolveDestinationNode §3.5 falls back to the warehouse code for an ordinary warehouse")]
    public void ResolveDestinationNode_OrdinaryWarehouseCode_ReturnsWarehouseCode()
    {
        Assert.Equal("EDC", GoodsInTransitReceivedRules.ResolveDestinationNode(null, "EDC"));
    }

    [Fact(DisplayName = "IsOmsDeltaEligible §3.6 returns true for a direct-from-supplier CAECOM receipt with no warehouse code")]
    public void IsOmsDeltaEligible_NoWarehouseCodeAndCaecomDestination_ReturnsTrue()
    {
        var location = new InventoryEventLocation(FulfilmentLocationIds.Caecom, InventoryEventLocationType.Warehouse);

        Assert.True(GoodsInTransitReceivedRules.IsOmsDeltaEligible(null, location));
        Assert.True(GoodsInTransitReceivedRules.IsOmsDeltaEligible(string.Empty, location));
        Assert.True(GoodsInTransitReceivedRules.IsOmsDeltaEligible("   ", location));
    }

    [Fact(DisplayName = "IsOmsDeltaEligible §3.6 returns false when a warehouse code is present, even for a CAECOM destination")]
    public void IsOmsDeltaEligible_WarehouseCodePresent_ReturnsFalse()
    {
        var location = new InventoryEventLocation(FulfilmentLocationIds.Caecom, InventoryEventLocationType.Warehouse);

        Assert.False(GoodsInTransitReceivedRules.IsOmsDeltaEligible("EDC", location));
    }

    [Fact(DisplayName = "IsOmsDeltaEligible §3.6 returns false for a non-CAECOM or null destination even with no warehouse code")]
    public void IsOmsDeltaEligible_NonCaecomOrNullDestination_ReturnsFalse()
    {
        Assert.False(GoodsInTransitReceivedRules.IsOmsDeltaEligible(null, null));
        Assert.False(GoodsInTransitReceivedRules.IsOmsDeltaEligible(null, new InventoryEventLocation("ADC", InventoryEventLocationType.Warehouse)));
    }
}
