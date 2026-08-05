using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="OrderStatusChangedRules"/> - warehouse classification (§3.1), reference-id
/// resolution (§3.2), and fulfilment-unit-id normalization (§3.4), including case-insensitive
/// matching of the TDC/ADC warehouse identifiers (§4).
/// </summary>
public class OrderStatusChangedRulesTests
{
    private static OrderStatusChangedEvent CreateEvent(string warehouseCode, string orderId = "order-1", string? pickingRouteId = null) => new(
        Channel: InventoryEventChannel.OwnOnline,
        Market: "DK",
        SellingLegalEntity: "PANDORA-DK",
        OrderId: orderId,
        BackOrderId: null,
        PickingRouteId: pickingRouteId,
        Status: OrderStatusCode.Completed,
        WarehouseCode: warehouseCode,
        IsReturn: false,
        ChangeDate: new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        CancelReason: null,
        SourceOrderReferenceId: null);

    [Theory(DisplayName = "IsNotTdcOrAdc §3.1 returns false for every TDC/ADC identifier, case-insensitively")]
    [InlineData("D001")]
    [InlineData("d001")]
    [InlineData("TDC")]
    [InlineData("tdc")]
    [InlineData("ADC")]
    [InlineData("adc")]
    public void IsNotTdcOrAdc_TdcOrAdcWarehouseCode_ReturnsFalse(string warehouseCode)
    {
        Assert.False(OrderStatusChangedRules.IsNotTdcOrAdc(warehouseCode));
    }

    [Fact(DisplayName = "IsNotTdcOrAdc §3.1 returns true for an ordinary warehouse code")]
    public void IsNotTdcOrAdc_StandardWarehouseCode_ReturnsTrue()
    {
        Assert.True(OrderStatusChangedRules.IsNotTdcOrAdc("WH-1"));
    }

    [Fact(DisplayName = "ResolveReferenceId §3.2 returns OrderId for a standard warehouse")]
    public void ResolveReferenceId_StandardWarehouse_ReturnsOrderId()
    {
        var target = CreateEvent(warehouseCode: "WH-1", orderId: "order-1", pickingRouteId: "route-1");

        Assert.Equal("order-1", OrderStatusChangedRules.ResolveReferenceId(target));
    }

    [Fact(DisplayName = "ResolveReferenceId §3.2 returns PickingRouteId for the TDC warehouse")]
    public void ResolveReferenceId_TdcWarehouse_ReturnsPickingRouteId()
    {
        var target = CreateEvent(warehouseCode: "TDC", orderId: "order-1", pickingRouteId: "route-1");

        Assert.Equal("route-1", OrderStatusChangedRules.ResolveReferenceId(target));
    }

    [Fact(DisplayName = "ResolveReferenceId §3.2 returns null for the ADC warehouse when PickingRouteId is null")]
    public void ResolveReferenceId_AdcWarehouseWithNullPickingRouteId_ReturnsNull()
    {
        var target = CreateEvent(warehouseCode: "ADC", orderId: "order-1", pickingRouteId: null);

        Assert.Null(OrderStatusChangedRules.ResolveReferenceId(target));
    }

    [Fact(DisplayName = "NormalizeFulfilmentUnitId §3.4 normalizes the TDC SAP id to TDC")]
    public void NormalizeFulfilmentUnitId_TdcSapId_ReturnsTdc()
    {
        Assert.Equal("TDC", OrderStatusChangedRules.NormalizeFulfilmentUnitId("D001"));
    }

    [Theory(DisplayName = "NormalizeFulfilmentUnitId §3.4 passes every other warehouse code through unchanged, including ADC")]
    [InlineData("ADC")]
    [InlineData("WH-1")]
    public void NormalizeFulfilmentUnitId_NonTdcSapWarehouseCode_ReturnsUnchanged(string warehouseCode)
    {
        Assert.Equal(warehouseCode, OrderStatusChangedRules.NormalizeFulfilmentUnitId(warehouseCode));
    }
}
