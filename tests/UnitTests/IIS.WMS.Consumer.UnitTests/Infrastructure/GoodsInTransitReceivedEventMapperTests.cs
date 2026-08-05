using IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived.Mappers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using net.pandora.nexus.shared;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="GoodsInTransitReceivedEventMapper"/> - the hand-written mapping from
/// the Avro-generated <see cref="net.pandora.nexus.@event.b2b.purchase.GoodsInTransitReceived"/> to this
/// consumer's own decoupled <see cref="GoodsInTransitReceivedEvent"/> wire contract
/// (docs/events/b2b.purchase.GoodsInTransitReceived.md).
/// </summary>
public class GoodsInTransitReceivedEventMapperTests
{
    private static net.pandora.nexus.@object.b2b.purchase.ShipmentLine CreateLine(
        string? lineNum = "1",
        string productId = "product-1",
        int quantity = 5,
        CountryCode? countryOfOrigin = CountryCode.DK,
        string? returnReasonCode = null,
        string? hallmarking = "585") => new()
    {
        lineNum = lineNum,
        productId = productId,
        quantity = quantity,
        countryOfOrigin = countryOfOrigin,
        returnReasonCode = returnReasonCode,
        hallmarking = hallmarking,
    };

    private static net.pandora.nexus.@event.b2b.purchase.GoodsInTransitReceived CreateSource(
        Location? locationTo = null,
        net.pandora.nexus.@object.b2b.purchase.ShipmentLine[]? shipmentLines = null) => new()
    {
        channel = Channel.INTERCOMPANY_DISTRIBUTION,
        shipment = new net.pandora.nexus.@object.b2b.purchase.Shipment
        {
            packingSlipId = "PS12345",
            receiptDate = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            warehouseCode = "EDC",
            vendorCode = "VENDOR-1",
            locationTo = locationTo,
            shipmentLines = shipmentLines ?? [CreateLine()],
        },
    };

    [Fact(DisplayName = "ToGoodsInTransitReceivedEvent maps channel via the shared InventoryStateChanged mapper")]
    public void ToGoodsInTransitReceivedEvent_Channel_MapsViaSharedMapper()
    {
        var result = CreateSource().ToGoodsInTransitReceivedEvent();

        Assert.Equal(InventoryEventChannel.IntercompanyDistribution, result.Channel);
    }

    [Fact(DisplayName = "ToGoodsInTransitReceivedEvent maps top-level shipment scalar fields as-is")]
    public void ToGoodsInTransitReceivedEvent_ShipmentScalarFields_MapsUnchanged()
    {
        var result = CreateSource().ToGoodsInTransitReceivedEvent();

        Assert.Equal("PS12345", result.Shipment.PackingSlipId);
        Assert.Equal(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc), result.Shipment.ReceiptDate);
        Assert.Equal("EDC", result.Shipment.WarehouseCode);
        Assert.Equal("VENDOR-1", result.Shipment.VendorCode);
    }

    [Fact(DisplayName = "ToGoodsInTransitReceivedEvent maps a present locationTo via the shared InventoryStateChanged location mapper")]
    public void ToGoodsInTransitReceivedEvent_PresentLocationTo_MapsViaSharedMapper()
    {
        var location = new Location { id = "CAECOM", type = LocationType.WAREHOUSE };

        var result = CreateSource(locationTo: location).ToGoodsInTransitReceivedEvent();

        Assert.NotNull(result.Shipment.LocationTo);
        Assert.Equal("CAECOM", result.Shipment.LocationTo!.Id);
        Assert.Equal(InventoryEventLocationType.Warehouse, result.Shipment.LocationTo.Type);
    }

    [Fact(DisplayName = "ToGoodsInTransitReceivedEvent maps a null locationTo to null")]
    public void ToGoodsInTransitReceivedEvent_NullLocationTo_MapsToNull()
    {
        var result = CreateSource(locationTo: null).ToGoodsInTransitReceivedEvent();

        Assert.Null(result.Shipment.LocationTo);
    }

    [Fact(DisplayName = "ToGoodsInTransitReceivedEvent maps every shipment line field, converting countryOfOrigin to its string representation")]
    public void ToGoodsInTransitReceivedEvent_ShipmentLine_MapsAllFields()
    {
        var line = CreateLine(
            lineNum: "1",
            productId: "product-1",
            quantity: 5,
            countryOfOrigin: CountryCode.DK,
            returnReasonCode: "DAMAGED",
            hallmarking: "585");

        var result = CreateSource(shipmentLines: [line]).ToGoodsInTransitReceivedEvent();

        var mappedLine = Assert.Single(result.Shipment.ShipmentLines);
        Assert.Equal("1", mappedLine.LineNum);
        Assert.Equal("product-1", mappedLine.ProductId);
        Assert.Equal(5, mappedLine.Quantity);
        Assert.Equal(CountryCode.DK.ToString(), mappedLine.CountryOfOrigin);
        Assert.Equal("DAMAGED", mappedLine.ReturnReasonCode);
        Assert.Equal("585", mappedLine.Hallmarking);
    }

    [Fact(DisplayName = "ToGoodsInTransitReceivedEvent maps a null shipment line countryOfOrigin/lineNum/returnReasonCode/hallmarking to null")]
    public void ToGoodsInTransitReceivedEvent_NullableShipmentLineFields_MapToNull()
    {
        var line = CreateLine(lineNum: null, countryOfOrigin: null, returnReasonCode: null, hallmarking: null);

        var result = CreateSource(shipmentLines: [line]).ToGoodsInTransitReceivedEvent();

        var mappedLine = Assert.Single(result.Shipment.ShipmentLines);
        Assert.Null(mappedLine.LineNum);
        Assert.Null(mappedLine.CountryOfOrigin);
        Assert.Null(mappedLine.ReturnReasonCode);
        Assert.Null(mappedLine.Hallmarking);
    }
}
