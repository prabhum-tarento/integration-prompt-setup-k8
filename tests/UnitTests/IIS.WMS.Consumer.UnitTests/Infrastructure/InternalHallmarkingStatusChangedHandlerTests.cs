using IIS.WMS.Consumer.Application.InternalHallmarkingStatusChanged;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Application.OrderTracking;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged.Handlers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using DomainEnums = IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="InternalHallmarkingStatusChangedHandler"/> - status-routed dispatch to the
/// four use-case methods, unrecognized-status skip (including its order-tracking publish), and the
/// downstream OMS-delta/ICR-snapshot/inventory-adjusted-reflex publish gating
/// (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.1-§3.5/§8/§9).
/// </summary>
public class InternalHallmarkingStatusChangedHandlerTests
{
    private static InternalHallmarkingStatusChangedEvent CreateEvent(
        Status status,
        string locationId = "WH-1",
        InventoryEventLocationType locationType = InventoryEventLocationType.Warehouse,
        string hallmarkingFrom = "NON",
        string hallmarkingTo = "925") => new(
        Channel: InventoryEventChannel.OwnOnline,
        Status: status,
        Id: "hallmark-1",
        ChangeDate: new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        Location: new InventoryEventLocation(locationId, locationType),
        Entity: "ORG-1",
        Type: InventoryEventChangeType.Mqa,
        InventoryState: new InventoryEventStateSnapshot(InventoryEventStockState.Available, InventoryEventStockStatus.Hallmarking),
        ItemLine: new HallmarkingItemLine(
            LineNum: "1",
            ProductId: "SKU-1",
            Quantity: 4,
            CountryOfOrigin: "TH",
            HallmarkingFrom: hallmarkingFrom,
            HallmarkingTo: hallmarkingTo,
            ReasonCode: "ALLOCATION"));

    private static InternalHallmarkingStatusChangedHandler CreateHandler(
        out IInternalHallmarkingStatusChangedService service,
        out IOrderTrackingPublisher orderTrackingPublisher,
        out IDeltaTowardsOmsPublisher deltaTowardsOmsPublisher,
        out IInventoryComparisonReportPublisher inventoryComparisonReportPublisher,
        out IInventoryAdjustedReflexPublisher inventoryAdjustedReflexPublisher,
        FeatureFlagsOptions? featureFlags = null,
        ItemStockInventoryDeltaResult? deltaResult = null)
    {
        service = Substitute.For<IInternalHallmarkingStatusChangedService>();
        var noDelta = new ItemStockInventoryDeltaResult { IsB2CChanged = false, DeltaTowardsOms = 0 };
        var result = deltaResult ?? noDelta;

        service.AllocateAsync(default!, default!, default!, default!, default, default).ReturnsForAnyArgs(result);
        service.PickAndShipAsync(default!, default!, default!, default!, default, default).ReturnsForAnyArgs(result);
        service.ChangeHallmarkAsync(default!, default!, default!, default!, default!, default, default, default).ReturnsForAnyArgs(result);
        service.CompleteTransitAsync(default!, default!, default!, default!, default, default).ReturnsForAnyArgs(Task.CompletedTask);

        orderTrackingPublisher = Substitute.For<IOrderTrackingPublisher>();
        deltaTowardsOmsPublisher = Substitute.For<IDeltaTowardsOmsPublisher>();
        inventoryComparisonReportPublisher = Substitute.For<IInventoryComparisonReportPublisher>();
        inventoryAdjustedReflexPublisher = Substitute.For<IInventoryAdjustedReflexPublisher>();

        var featureFlagsOptions = Substitute.For<IOptions<FeatureFlagsOptions>>();
        featureFlagsOptions.Value.Returns(featureFlags ?? new FeatureFlagsOptions());

        return new InternalHallmarkingStatusChangedHandler(
            service,
            orderTrackingPublisher,
            deltaTowardsOmsPublisher,
            inventoryComparisonReportPublisher,
            inventoryAdjustedReflexPublisher,
            featureFlagsOptions,
            Substitute.For<ILogger<InternalHallmarkingStatusChangedHandler>>());
    }

    [Fact(DisplayName = "HandleAsync §3.1 routes a Started status to AllocateAsync")]
    public async Task HandleAsync_StartedStatus_RoutesToAllocateAsync()
    {
        var target = CreateEvent(Status.Started);
        var sut = CreateHandler(out var service, out _, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await service.Received(1).AllocateAsync("WH-1", "SKU-1", "TH", "925", 4, Arg.Any<CancellationToken>());
        await service.DidNotReceiveWithAnyArgs().PickAndShipAsync(default!, default!, default!, default!, default, default);
        await service.DidNotReceiveWithAnyArgs().ChangeHallmarkAsync(default!, default!, default!, default!, default!, default, default, default);
        await service.DidNotReceiveWithAnyArgs().CompleteTransitAsync(default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync §3.2/§3.3 routes a Picked status to PickAndShipAsync")]
    public async Task HandleAsync_PickedStatus_RoutesToPickAndShipAsync()
    {
        var target = CreateEvent(Status.Picked);
        var sut = CreateHandler(out var service, out _, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await service.Received(1).PickAndShipAsync("WH-1", "SKU-1", "TH", "925", 4, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.4 routes a Changed status to ChangeHallmarkAsync with the third-party-logistics flag derived from location type")]
    public async Task HandleAsync_ChangedStatusThirdPartyLogisticsLocation_RoutesToChangeHallmarkAsyncWithFlagTrue()
    {
        var target = CreateEvent(Status.Changed, locationType: InventoryEventLocationType.ThirdPartyLogistics, hallmarkingFrom: "NON", hallmarkingTo: "925");
        var sut = CreateHandler(out var service, out _, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await service.Received(1).ChangeHallmarkAsync("WH-1", "SKU-1", "TH", "NON", "925", 4, true, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.4 routes a Changed status to ChangeHallmarkAsync with the third-party-logistics flag false for a warehouse location")]
    public async Task HandleAsync_ChangedStatusWarehouseLocation_RoutesToChangeHallmarkAsyncWithFlagFalse()
    {
        var target = CreateEvent(Status.Changed, locationType: InventoryEventLocationType.Warehouse);
        var sut = CreateHandler(out var service, out _, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await service.Received(1).ChangeHallmarkAsync("WH-1", "SKU-1", "TH", "NON", "925", 4, false, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.5 routes a Finished status to CompleteTransitAsync and publishes to inventory-adjusted-reflex unconditionally")]
    public async Task HandleAsync_FinishedStatus_RoutesToCompleteTransitAsyncAndPublishesReflex()
    {
        var target = CreateEvent(Status.Finished);
        var sut = CreateHandler(out var service, out _, out _, out _, out var inventoryAdjustedReflexPublisher);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await service.Received(1).CompleteTransitAsync("WH-1", "SKU-1", "TH", "925", 4, Arg.Any<CancellationToken>());
        await inventoryAdjustedReflexPublisher.Received(1).PublishAsync(
            "OwnOnline", "hallmark-1", target.ChangeDate, "WH-1", "Warehouse", "ORG-1",
            "SKU-1", 4, "TH", "925",
            DomainEnums.State.AVAILABLE, DomainEnums.Status.HALLMARKING,
            "hallmark-1", Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync skips every service call, publish, and order-tracking for an unrecognized status")]
    public async Task HandleAsync_UnrecognizedStatus_SkipsEverythingIncludingOrderTracking()
    {
        var target = CreateEvent(Status.Unknown);
        var sut = CreateHandler(
            out var service, out var orderTrackingPublisher, out _, out _, out var inventoryAdjustedReflexPublisher);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await service.DidNotReceiveWithAnyArgs().AllocateAsync(default!, default!, default!, default!, default, default);
        await service.DidNotReceiveWithAnyArgs().PickAndShipAsync(default!, default!, default!, default!, default, default);
        await service.DidNotReceiveWithAnyArgs().ChangeHallmarkAsync(default!, default!, default!, default!, default!, default, default, default);
        await service.DidNotReceiveWithAnyArgs().CompleteTransitAsync(default!, default!, default!, default!, default, default);
        await inventoryAdjustedReflexPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default, default!, default!, default, default!, default, default!, default!, default, default, default!, default);
        await orderTrackingPublisher.DidNotReceiveWithAnyArgs().PublishAsync(default!, default);
    }

    [Theory(DisplayName = "HandleAsync publishes order-tracking with the mapped OrderTrackingStatus for every recognized status")]
    [InlineData(Status.Started, OrderTrackingStatus.ALLOCATED)]
    [InlineData(Status.Picked, OrderTrackingStatus.PICKED)]
    [InlineData(Status.Changed, OrderTrackingStatus.INTRANSIT)]
    [InlineData(Status.Finished, OrderTrackingStatus.SHIPPED)]
    public async Task HandleAsync_RecognizedStatus_PublishesOrderTrackingWithMappedStatus(Status status, OrderTrackingStatus expected)
    {
        var target = CreateEvent(status);
        var sut = CreateHandler(out _, out var orderTrackingPublisher, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.Received(1).PublishAsync(
            Arg.Is<OrderTrackingRelayRequest>(r =>
                r.ReferenceId == "hallmark-1" &&
                r.Channel == "OwnOnline" &&
                r.FulfilmentUnitId == "WH-1" &&
                r.FulfilmentUnitType == "Warehouse" &&
                r.FunctionName == nameof(InternalHallmarkingStatusChangedHandler) &&
                r.OrderId == "hallmark-1" &&
                r.OrderStatus == expected &&
                r.OrderType == OrderType.INTERNALHALLMARKING.ToString() &&
                r.Lines.Count == 1 &&
                r.Lines[0].ItemCode == "SKU-1" &&
                r.Lines[0].CountryOfOrigin == "TH" &&
                r.Lines[0].HallMarkType == "925" &&
                r.Lines[0].Qty == 4),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.1 publishes the OMS delta after Started when IsB2CChanged and EnableDeltaTowardsOms are both set")]
    public async Task HandleAsync_StartedB2CChangedWithEnableDeltaTowardsOms_PublishesOmsDelta()
    {
        var target = CreateEvent(Status.Started);
        var sut = CreateHandler(
            out _, out _, out var deltaTowardsOmsPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsOms = true },
            deltaResult: new ItemStockInventoryDeltaResult { IsB2CChanged = true, DeltaTowardsOms = 260 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.Received(1).PublishAsync(
            "SKU-1", "WH-1", "Warehouse", "TH", "925", 260, "hallmark-1", Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync does not publish the OMS delta when IsB2CChanged is true but EnableDeltaTowardsOms is disabled")]
    public async Task HandleAsync_B2CChangedButFlagDisabled_SkipsOmsDeltaPublish()
    {
        var target = CreateEvent(Status.Started);
        var sut = CreateHandler(
            out _, out _, out var deltaTowardsOmsPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions(),
            deltaResult: new ItemStockInventoryDeltaResult { IsB2CChanged = true, DeltaTowardsOms = 260 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync does not publish the OMS delta when EnableDeltaTowardsOms is set but IsB2CChanged is false")]
    public async Task HandleAsync_EnabledButB2CNotChanged_SkipsOmsDeltaPublish()
    {
        var target = CreateEvent(Status.Started);
        var sut = CreateHandler(
            out _, out _, out var deltaTowardsOmsPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsOms = true },
            deltaResult: new ItemStockInventoryDeltaResult { IsB2CChanged = false, DeltaTowardsOms = 0 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync §3.2 publishes the ICR snapshot on Picked when EnableSnapshotForIcr is set, using the Caecom flag from location id")]
    public async Task HandleAsync_PickedWithEnableSnapshotForIcrCaecomLocation_PublishesIcrSnapshot()
    {
        var target = CreateEvent(Status.Picked, locationId: FulfilmentLocationIds.Caecom);
        var sut = CreateHandler(
            out _, out _, out _, out var inventoryComparisonReportPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableSnapshotForIcr = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryComparisonReportPublisher.Received(1).PublishAsync(
            FulfilmentLocationIds.Caecom, "SKU-1", "925", "TH", true, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.4 publishes the ICR snapshot on Changed when EnableSnapshotForIcr is set")]
    public async Task HandleAsync_ChangedWithEnableSnapshotForIcr_PublishesIcrSnapshot()
    {
        var target = CreateEvent(Status.Changed, locationId: "WH-1");
        var sut = CreateHandler(
            out _, out _, out _, out var inventoryComparisonReportPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableSnapshotForIcr = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryComparisonReportPublisher.Received(1).PublishAsync(
            "WH-1", "SKU-1", "925", "TH", false, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync does not publish the ICR snapshot on Started even when EnableSnapshotForIcr is set")]
    public async Task HandleAsync_StartedWithEnableSnapshotForIcr_SkipsIcrSnapshotPublish()
    {
        var target = CreateEvent(Status.Started);
        var sut = CreateHandler(
            out _, out _, out _, out var inventoryComparisonReportPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableSnapshotForIcr = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryComparisonReportPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync does not publish the ICR snapshot on Finished even when EnableSnapshotForIcr is set")]
    public async Task HandleAsync_FinishedWithEnableSnapshotForIcr_SkipsIcrSnapshotPublish()
    {
        var target = CreateEvent(Status.Finished);
        var sut = CreateHandler(
            out _, out _, out _, out var inventoryComparisonReportPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableSnapshotForIcr = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryComparisonReportPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default, default);
    }
}
