using IIS.WMS.Consumer.Application.OrderTracking;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged.Handlers;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="OrderStatusChangedHandler"/> - reference-id resolution (standard warehouse vs.
/// TDC/ADC), fulfilment-unit-id normalization, status mapping, and the empty-reference-id skip
/// (docs/events/b2b.sales.OrderStatusChanged.md §3.1-§3.4/§6).
/// </summary>
public class OrderStatusChangedHandlerTests
{
    private static OrderStatusChangedEvent CreateEvent(
        string warehouseCode = "WH-1",
        string orderId = "order-1",
        string? pickingRouteId = null,
        OrderStatusCode status = OrderStatusCode.Completed) => new(
        Channel: InventoryEventChannel.OwnOnline,
        Market: "DK",
        SellingLegalEntity: "PANDORA-DK",
        OrderId: orderId,
        BackOrderId: null,
        PickingRouteId: pickingRouteId,
        Status: status,
        WarehouseCode: warehouseCode,
        IsReturn: false,
        ChangeDate: new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        CancelReason: null,
        SourceOrderReferenceId: null);

    private static OrderStatusChangedHandler CreateHandler(out IOrderTrackingPublisher orderTrackingPublisher)
    {
        orderTrackingPublisher = Substitute.For<IOrderTrackingPublisher>();

        return new OrderStatusChangedHandler(
            orderTrackingPublisher,
            Substitute.For<ILogger<OrderStatusChangedHandler>>());
    }

    [Fact(DisplayName = "HandleAsync §3.2 publishes order-tracking using OrderId as the reference id for a standard warehouse")]
    public async Task HandleAsync_StandardWarehouse_PublishesUsingOrderIdAsReferenceId()
    {
        var target = CreateEvent(warehouseCode: "WH-1", orderId: "order-1");
        var sut = CreateHandler(out var orderTrackingPublisher);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.Received(1).PublishAsync(
            Arg.Is<OrderTrackingRelayRequest>(r =>
                r.ReferenceId == "order-1" &&
                r.OrderId == "order-1" &&
                r.FulfilmentUnitId == "WH-1"),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.1/§3.2 publishes order-tracking using PickingRouteId as the reference id for the ADC warehouse")]
    public async Task HandleAsync_AdcWarehouse_PublishesUsingPickingRouteIdAsReferenceId()
    {
        var target = CreateEvent(warehouseCode: "ADC", orderId: "order-1", pickingRouteId: "route-1");
        var sut = CreateHandler(out var orderTrackingPublisher);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.Received(1).PublishAsync(
            Arg.Is<OrderTrackingRelayRequest>(r =>
                r.ReferenceId == "route-1" &&
                r.OrderId == "route-1" &&
                r.FulfilmentUnitId == "ADC"),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.4 normalizes the TDC SAP warehouse id to TDC on the published fulfilment unit id")]
    public async Task HandleAsync_TdcSapWarehouseCode_NormalizesFulfilmentUnitIdToTdc()
    {
        var target = CreateEvent(warehouseCode: "D001", orderId: "order-1", pickingRouteId: "route-1");
        var sut = CreateHandler(out var orderTrackingPublisher);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.Received(1).PublishAsync(
            Arg.Is<OrderTrackingRelayRequest>(r => r.FulfilmentUnitId == "TDC"),
            Arg.Any<CancellationToken>());
    }

    [Theory(DisplayName = "HandleAsync maps Cancelled/Deleted statuses and defaults every other status to UNKNOWN")]
    [InlineData(OrderStatusCode.Cancelled, OrderTrackingStatus.CANCELLED)]
    [InlineData(OrderStatusCode.Deleted, OrderTrackingStatus.DELETED)]
    [InlineData(OrderStatusCode.Completed, OrderTrackingStatus.UNKNOWN)]
    [InlineData(OrderStatusCode.OrderCanceled, OrderTrackingStatus.UNKNOWN)]
    public async Task HandleAsync_GivenStatus_MapsToExpectedOrderTrackingStatus(OrderStatusCode status, OrderTrackingStatus expected)
    {
        var target = CreateEvent(status: status);
        var sut = CreateHandler(out var orderTrackingPublisher);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.Received(1).PublishAsync(
            Arg.Is<OrderTrackingRelayRequest>(r => r.OrderStatus == expected),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync skips the order-tracking publish when the ADC/TDC reference id (PickingRouteId) is null")]
    public async Task HandleAsync_AdcWarehouseWithNullPickingRouteId_SkipsPublish()
    {
        var target = CreateEvent(warehouseCode: "ADC", pickingRouteId: null);
        var sut = CreateHandler(out var orderTrackingPublisher);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.DidNotReceiveWithAnyArgs().PublishAsync(default!, default);
    }
}
