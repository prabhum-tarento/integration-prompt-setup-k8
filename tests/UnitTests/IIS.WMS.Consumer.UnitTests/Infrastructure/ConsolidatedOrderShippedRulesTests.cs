using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="ConsolidatedOrderShippedRules"/> - warehouse classification, per-line
/// grouping-key resolution, packing-slip-id assignment, and order-status/eligibility rules
/// (docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.2), including case-insensitive matching of
/// the TDC/ADC warehouse identifiers (§4).
/// </summary>
public class ConsolidatedOrderShippedRulesTests
{
    private static ConsolidatedOrderShipmentLine CreateLine(string orderId = "order-1", string pickingRouteId = "route-1") => new(
        LineNum: "1",
        PickingRouteId: pickingRouteId,
        OrderId: orderId,
        LotId: "lot-1",
        ProductId: "item-1",
        Quantity: 4,
        Hallmarking: "925",
        AllocatedFromB2BBucketQuantity: 4,
        CountryOfOrigin: "TH");

    [Theory(DisplayName = "IsNotTdcOrAdc §3.2 returns false for every TDC/ADC identifier, case-insensitively")]
    [InlineData("D001")]
    [InlineData("d001")]
    [InlineData("TDC")]
    [InlineData("tdc")]
    [InlineData("ADC")]
    [InlineData("adc")]
    public void IsNotTdcOrAdc_TdcOrAdcWarehouseCode_ReturnsFalse(string warehouseCode)
    {
        Assert.False(ConsolidatedOrderShippedRules.IsNotTdcOrAdc(warehouseCode));
    }

    [Fact(DisplayName = "IsNotTdcOrAdc §3.2 returns true for an ordinary warehouse code")]
    public void IsNotTdcOrAdc_StandardWarehouseCode_ReturnsTrue()
    {
        Assert.True(ConsolidatedOrderShippedRules.IsNotTdcOrAdc("WH-1"));
    }

    [Fact(DisplayName = "ResolveGroupKey §3.2 returns OrderId for a 3PL (non-TDC/ADC) warehouse")]
    public void ResolveGroupKey_ThirdPartyLogisticsWarehouse_ReturnsOrderId()
    {
        var line = CreateLine(orderId: "order-1", pickingRouteId: "route-1");

        Assert.Equal("order-1", ConsolidatedOrderShippedRules.ResolveGroupKey("WH-1", line));
    }

    [Theory(DisplayName = "ResolveGroupKey §3.2 returns PickingRouteId for TDC/ADC warehouses")]
    [InlineData("TDC")]
    [InlineData("ADC")]
    [InlineData("D001")]
    public void ResolveGroupKey_TdcOrAdcWarehouse_ReturnsPickingRouteId(string warehouseCode)
    {
        var line = CreateLine(orderId: "order-1", pickingRouteId: "route-1");

        Assert.Equal("route-1", ConsolidatedOrderShippedRules.ResolveGroupKey(warehouseCode, line));
    }

    [Theory(DisplayName = "ResolvePackingSlipId §3.2 returns ParentOrderId for the TDC warehouse, case-insensitively, by either identifier")]
    [InlineData("TDC")]
    [InlineData("tdc")]
    [InlineData("D001")]
    [InlineData("d001")]
    public void ResolvePackingSlipId_TdcWarehouse_ReturnsParentOrderId(string warehouseCode)
    {
        Assert.Equal(
            "parent-order-1",
            ConsolidatedOrderShippedRules.ResolvePackingSlipId(warehouseCode, "parent-order-1", "shipment-packing-slip-1"));
    }

    [Theory(DisplayName = "ResolvePackingSlipId §3.2 returns the shipment's own packing slip id for every other warehouse, including ADC")]
    [InlineData("ADC")]
    [InlineData("WH-1")]
    public void ResolvePackingSlipId_NonTdcWarehouse_ReturnsShipmentPackingSlipId(string warehouseCode)
    {
        Assert.Equal(
            "shipment-packing-slip-1",
            ConsolidatedOrderShippedRules.ResolvePackingSlipId(warehouseCode, "parent-order-1", "shipment-packing-slip-1"));
    }

    [Fact(DisplayName = "ResolveOrderStatus §3.2 returns SHIPPED for a non-preliminary confirmation")]
    public void ResolveOrderStatus_NonPreliminaryConfirmation_ReturnsShipped()
    {
        Assert.Equal(
            OrderTrackingStatus.SHIPPED,
            ConsolidatedOrderShippedRules.ResolveOrderStatus(ConfirmationType.STANDARD, isExport: false));
    }

    [Fact(DisplayName = "ResolveOrderStatus §3.2 returns SHIPPED for a preliminary confirmation that is not an export")]
    public void ResolveOrderStatus_PreliminaryNonExport_ReturnsShipped()
    {
        Assert.Equal(
            OrderTrackingStatus.SHIPPED,
            ConsolidatedOrderShippedRules.ResolveOrderStatus(ConfirmationType.PRELIMINARY, isExport: false));
    }

    [Fact(DisplayName = "ResolveOrderStatus §3.2 returns INVOICED for a preliminary export confirmation")]
    public void ResolveOrderStatus_PreliminaryExport_ReturnsInvoiced()
    {
        Assert.Equal(
            OrderTrackingStatus.INVOICED,
            ConsolidatedOrderShippedRules.ResolveOrderStatus(ConfirmationType.PRELIMINARY, isExport: true));
    }

    [Fact(DisplayName = "IsOrderTrackingEligible §3.2 returns true for a non-preliminary confirmation")]
    public void IsOrderTrackingEligible_NonPreliminaryConfirmation_ReturnsTrue()
    {
        Assert.True(ConsolidatedOrderShippedRules.IsOrderTrackingEligible(ConfirmationType.STANDARD, isExport: false));
    }

    [Fact(DisplayName = "IsOrderTrackingEligible §3.2 returns false for a preliminary confirmation that is not an export")]
    public void IsOrderTrackingEligible_PreliminaryNonExport_ReturnsFalse()
    {
        Assert.False(ConsolidatedOrderShippedRules.IsOrderTrackingEligible(ConfirmationType.PRELIMINARY, isExport: false));
    }

    [Fact(DisplayName = "IsOrderTrackingEligible §3.2 returns true for a preliminary export confirmation")]
    public void IsOrderTrackingEligible_PreliminaryExport_ReturnsTrue()
    {
        Assert.True(ConsolidatedOrderShippedRules.IsOrderTrackingEligible(ConfirmationType.PRELIMINARY, isExport: true));
    }
}
