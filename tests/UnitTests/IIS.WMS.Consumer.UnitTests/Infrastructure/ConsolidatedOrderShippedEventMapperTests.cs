using IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped.Mappers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using DomainConfirmationType = IIS.WMS.Consumer.Domain.Enums.ConfirmationType;
using net.pandora.nexus.shared;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="ConsolidatedOrderShippedEventMapper"/> - the hand-written mapping
/// from the Avro-generated <see cref="net.pandora.nexus.@event.b2b.sales.ConsolidatedOrderShipped"/> to
/// this consumer's own decoupled <see cref="ConsolidatedOrderShippedEvent"/> wire contract, focused on
/// the nullable <c>market</c> field, the nested <c>Shipment</c>/<c>ShipmentLine</c> mapping, and the
/// full <c>ConfirmationType</c> symbol mapping.
/// </summary>
public class ConsolidatedOrderShippedEventMapperTests
{
    private static net.pandora.nexus.@event.b2b.sales.ConsolidatedOrderShipped CreateSource(
        net.pandora.nexus.@event.b2b.sales.ConfirmationType confirmationType = net.pandora.nexus.@event.b2b.sales.ConfirmationType.STANDARD,
        CountryCode? market = CountryCode.DK) => new()
    {
        channel = Channel.OWN_ONLINE,
        market = market,
        parentOrderId = "parent-order-1",
        isExport = true,
        shipment = new()
        {
            id = "shipment-1",
            warehouseCode = "WH-1",
            confirmationType = confirmationType,
            shipDate = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            packingSlipId = "packing-slip-1",
            shipmentLines =
            [
                new()
                {
                    lineNum = "1",
                    pickingRouteId = "route-1",
                    orderId = "order-1",
                    lotId = "lot-1",
                    productId = "item-1",
                    quantity = 4,
                    hallmarking = "925",
                    allocatedFromB2BBucketQuantity = 4,
                    countryOfOrigin = CountryCode.TH,
                },
            ],
        },
    };

    [Fact(DisplayName = "ToConsolidatedOrderShippedEvent maps top-level scalar fields as-is")]
    public void ToConsolidatedOrderShippedEvent_ScalarFields_MapsUnchanged()
    {
        var result = CreateSource().ToConsolidatedOrderShippedEvent();

        Assert.Equal("parent-order-1", result.ParentOrderId);
        Assert.True(result.IsExport);
    }

    [Fact(DisplayName = "ToConsolidatedOrderShippedEvent maps channel via the shared InventoryStateChanged mapper")]
    public void ToConsolidatedOrderShippedEvent_Channel_MapsViaSharedMapper()
    {
        var result = CreateSource().ToConsolidatedOrderShippedEvent();

        Assert.Equal(InventoryEventChannel.OwnOnline, result.Channel);
    }

    [Fact(DisplayName = "ToConsolidatedOrderShippedEvent maps a present market to its string representation")]
    public void ToConsolidatedOrderShippedEvent_PresentMarket_MapsToStringRepresentation()
    {
        var result = CreateSource(market: CountryCode.DK).ToConsolidatedOrderShippedEvent();

        Assert.Equal(CountryCode.DK.ToString(), result.Market);
    }

    [Fact(DisplayName = "ToConsolidatedOrderShippedEvent maps a null market to null")]
    public void ToConsolidatedOrderShippedEvent_NullMarket_MapsToNull()
    {
        var result = CreateSource(market: null).ToConsolidatedOrderShippedEvent();

        Assert.Null(result.Market);
    }

    [Fact(DisplayName = "ToConsolidatedOrderShippedEvent maps the nested Shipment's scalar fields as-is")]
    public void ToConsolidatedOrderShippedEvent_Shipment_MapsScalarFields()
    {
        var result = CreateSource().ToConsolidatedOrderShippedEvent();

        Assert.Equal("shipment-1", result.Shipment.Id);
        Assert.Equal("WH-1", result.Shipment.WarehouseCode);
        Assert.Equal(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc), result.Shipment.ShipDate);
        Assert.Equal("packing-slip-1", result.Shipment.PackingSlipId);
    }

    [Fact(DisplayName = "ToConsolidatedOrderShippedEvent maps each ShipmentLine's fields as-is")]
    public void ToConsolidatedOrderShippedEvent_ShipmentLine_MapsFields()
    {
        var result = CreateSource().ToConsolidatedOrderShippedEvent();
        var line = Assert.Single(result.Shipment.ShipmentLines);

        Assert.Equal("1", line.LineNum);
        Assert.Equal("route-1", line.PickingRouteId);
        Assert.Equal("order-1", line.OrderId);
        Assert.Equal("lot-1", line.LotId);
        Assert.Equal("item-1", line.ProductId);
        Assert.Equal(4, line.Quantity);
        Assert.Equal("925", line.Hallmarking);
        Assert.Equal(4, line.AllocatedFromB2BBucketQuantity);
        Assert.Equal(CountryCode.TH.ToString(), line.CountryOfOrigin);
    }

    [Fact(DisplayName = "ToConsolidatedOrderShippedEvent maps a null CountryOfOrigin to null")]
    public void ToConsolidatedOrderShippedEvent_NullCountryOfOrigin_MapsToNull()
    {
        var source = CreateSource();
        source.shipment.shipmentLines[0].countryOfOrigin = null;

        var result = source.ToConsolidatedOrderShippedEvent();

        Assert.Null(result.Shipment.ShipmentLines[0].CountryOfOrigin);
    }

    [Theory(DisplayName = "ToConsolidatedOrderShippedEvent maps every ConfirmationType symbol by name")]
    [InlineData(net.pandora.nexus.@event.b2b.sales.ConfirmationType.UNKNOWN, DomainConfirmationType.UNKNOWN)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.ConfirmationType.PRELIMINARY, DomainConfirmationType.PRELIMINARY)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.ConfirmationType.STANDARD, DomainConfirmationType.STANDARD)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.ConfirmationType.STANDARD_FOLLOWING_PRELIMINARY, DomainConfirmationType.STANDARD_FOLLOWING_PRELIMINARY)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.ConfirmationType.PRELIMINARY_INVOICE, DomainConfirmationType.PRELIMINARY_INVOICE)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.ConfirmationType.PRELIMINARY_EXPORT, DomainConfirmationType.PRELIMINARY_EXPORT)]
    public void ToConsolidatedOrderShippedEvent_ConfirmationType_MapsBySymbolName(
        net.pandora.nexus.@event.b2b.sales.ConfirmationType source, DomainConfirmationType expected)
    {
        var result = CreateSource(source).ToConsolidatedOrderShippedEvent();

        Assert.Equal(expected, result.Shipment.ConfirmationType);
    }
}
