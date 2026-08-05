using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged.Mappers;
using net.pandora.nexus.shared;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="OrderStatusChangedEventMapper"/> - the hand-written mapping from
/// the Avro-generated <see cref="net.pandora.nexus.@event.b2b.sales.OrderStatusChanged"/> to this
/// consumer's own decoupled <see cref="OrderStatusChangedEvent"/> wire contract, focused on the nullable
/// <c>market</c> field and the full <c>StatusCode</c> symbol mapping.
/// </summary>
public class OrderStatusChangedEventMapperTests
{
    private static net.pandora.nexus.@event.b2b.sales.OrderStatusChanged CreateSource(
        net.pandora.nexus.@event.b2b.sales.StatusCode status = net.pandora.nexus.@event.b2b.sales.StatusCode.COMPLETED,
        CountryCode? market = CountryCode.DK) => new()
    {
        channel = Channel.OWN_ONLINE,
        market = market,
        sellingLegalEntity = "PANDORA-DK",
        orderId = "order-1",
        backOrderId = "backorder-1",
        pickingRouteId = "route-1",
        status = status,
        warehouseCode = "WH-1",
        isReturn = false,
        changeDate = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        cancelReason = "customer-requested",
        sourceOrderReferenceId = "src-ref-1",
    };

    [Fact(DisplayName = "ToOrderStatusChangedEvent maps top-level scalar fields as-is")]
    public void ToOrderStatusChangedEvent_ScalarFields_MapsUnchanged()
    {
        var result = CreateSource().ToOrderStatusChangedEvent();

        Assert.Equal("order-1", result.OrderId);
        Assert.Equal("backorder-1", result.BackOrderId);
        Assert.Equal("route-1", result.PickingRouteId);
        Assert.Equal("WH-1", result.WarehouseCode);
        Assert.Equal(false, result.IsReturn);
        Assert.Equal(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc), result.ChangeDate);
        Assert.Equal("customer-requested", result.CancelReason);
        Assert.Equal("src-ref-1", result.SourceOrderReferenceId);
        Assert.Equal("PANDORA-DK", result.SellingLegalEntity);
    }

    [Fact(DisplayName = "ToOrderStatusChangedEvent maps a present market to its string representation")]
    public void ToOrderStatusChangedEvent_PresentMarket_MapsToStringRepresentation()
    {
        var result = CreateSource(market: CountryCode.DK).ToOrderStatusChangedEvent();

        Assert.Equal(CountryCode.DK.ToString(), result.Market);
    }

    [Fact(DisplayName = "ToOrderStatusChangedEvent maps a null market to null")]
    public void ToOrderStatusChangedEvent_NullMarket_MapsToNull()
    {
        var result = CreateSource(market: null).ToOrderStatusChangedEvent();

        Assert.Null(result.Market);
    }

    [Fact(DisplayName = "ToOrderStatusChangedEvent maps channel via the shared InventoryStateChanged mapper")]
    public void ToOrderStatusChangedEvent_Channel_MapsViaSharedMapper()
    {
        var result = CreateSource().ToOrderStatusChangedEvent();

        Assert.Equal(InventoryEventChannel.OwnOnline, result.Channel);
    }

    [Theory(DisplayName = "ToOrderStatusChangedEvent maps every StatusCode symbol by name")]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.UNKNOWN, OrderStatusCode.Unknown)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.DEACTIVATED, OrderStatusCode.Deactivated)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.NOT_RUN, OrderStatusCode.NotRun)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.RUN, OrderStatusCode.Run)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.COLLECTION_STARTED, OrderStatusCode.CollectionStarted)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.COLLECTION_PERFORMED, OrderStatusCode.CollectionPerformed)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.PREPARATION_IN_PROGRESS, OrderStatusCode.PreparationInProgress)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.TO_PACKAGE, OrderStatusCode.ToPackage)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.COMPLETED, OrderStatusCode.Completed)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.DESPATCHED, OrderStatusCode.Despatched)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.CANCELLED, OrderStatusCode.Cancelled)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.DELETED, OrderStatusCode.Deleted)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.ORDER_CANCELED, OrderStatusCode.OrderCanceled)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.CREDIT_BLOCKED, OrderStatusCode.CreditBlocked)]
    [InlineData(net.pandora.nexus.@event.b2b.sales.StatusCode.CREDIT_UNBLOCKED, OrderStatusCode.CreditUnblocked)]
    public void ToOrderStatusChangedEvent_StatusCode_MapsBySymbolName(
        net.pandora.nexus.@event.b2b.sales.StatusCode source, OrderStatusCode expected)
    {
        var result = CreateSource(source).ToOrderStatusChangedEvent();

        Assert.Equal(expected, result.Status);
    }
}
