using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Application.OrderTracking;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived.Handlers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="GoodsInTransitReceivedHandler"/> - per-line service dispatch, the §3.6 OMS-delta
/// publish gate, and the single order-tracking publish covering every line
/// (docs/events/b2b.purchase.GoodsInTransitReceived.md §3.6/§3.7/§7.3).
/// </summary>
public class GoodsInTransitReceivedHandlerTests
{
    private static GoodsInTransitReceivedEvent CreateEvent(
        string packingSlipId = "PS12345",
        string warehouseCode = "",
        InventoryEventLocation? locationTo = null,
        IReadOnlyList<GoodsInTransitShipmentLine>? shipmentLines = null) => new(
        Channel: InventoryEventChannel.IntercompanyDistribution,
        Shipment: new GoodsInTransitShipment(
            PackingSlipId: packingSlipId,
            ReceiptDate: new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            WarehouseCode: warehouseCode,
            VendorCode: "VENDOR-1",
            LocationTo: locationTo ?? new InventoryEventLocation(FulfilmentLocationIds.Caecom, InventoryEventLocationType.Warehouse),
            ShipmentLines: shipmentLines ?? [CreateLine()]));

    private static GoodsInTransitShipmentLine CreateLine(
        string productId = "SKU-1",
        int quantity = 5,
        string? countryOfOrigin = "DK",
        string? returnReasonCode = null,
        string? hallmarking = "585") => new(
        LineNum: "1",
        ProductId: productId,
        Quantity: quantity,
        CountryOfOrigin: countryOfOrigin,
        ReturnReasonCode: returnReasonCode,
        Hallmarking: hallmarking);

    private static GoodsInTransitReceivedHandler CreateHandler(
        out IGoodsInTransitReceivedService goodsInTransitReceivedService,
        out IDeltaTowardsOmsPublisher deltaTowardsOmsPublisher,
        out IOrderTrackingPublisher orderTrackingPublisher,
        FeatureFlagsOptions? featureFlags = null,
        GoodsInTransitReceiptResult? serviceResult = null)
    {
        goodsInTransitReceivedService = Substitute.For<IGoodsInTransitReceivedService>();
        goodsInTransitReceivedService
            .ReceiveShipmentLineAsync(default!, default!, default!, default!, default, default, default, default, default)
            .ReturnsForAnyArgs(serviceResult ?? new GoodsInTransitReceiptResult());

        deltaTowardsOmsPublisher = Substitute.For<IDeltaTowardsOmsPublisher>();
        orderTrackingPublisher = Substitute.For<IOrderTrackingPublisher>();

        var archiveWriter = Substitute.For<IMessageArchiveWriter>();

        var featureFlagsOptions = Substitute.For<IOptions<FeatureFlagsOptions>>();
        featureFlagsOptions.Value.Returns(featureFlags ?? new FeatureFlagsOptions());

        return new GoodsInTransitReceivedHandler(
            goodsInTransitReceivedService,
            deltaTowardsOmsPublisher,
            orderTrackingPublisher,
            archiveWriter,
            featureFlagsOptions,
            Substitute.For<ILogger<GoodsInTransitReceivedHandler>>());
    }

    [Fact(DisplayName = "HandleAsync calls ReceiveShipmentLineAsync once per shipment line with the rule-resolved parameters")]
    public async Task HandleAsync_MultipleShipmentLines_CallsServiceOncePerLineWithResolvedParameters()
    {
        var target = CreateEvent(shipmentLines:
        [
            CreateLine(productId: "SKU-1", quantity: 5, returnReasonCode: null),
            CreateLine(productId: "SKU-2", quantity: 3, returnReasonCode: "DAMAGED"),
        ]);
        var sut = CreateHandler(out var service, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await service.Received(1).ReceiveShipmentLineAsync(
            FulfilmentLocationIds.Caecom, "SKU-1", "DK", "585", 5, true, State.AVAILABLE, Status.HELD, Arg.Any<CancellationToken>());
        await service.Received(1).ReceiveShipmentLineAsync(
            FulfilmentLocationIds.Caecom, "SKU-2", "DK", "585", 3, true, State.INSPECTION, Status.HELD, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync defaults a null CountryOfOrigin/Hallmarking to UNKNOWN/NON")]
    public async Task HandleAsync_NullCountryOfOriginAndHallmarking_DefaultsToFallbackValues()
    {
        var target = CreateEvent(shipmentLines: [CreateLine(countryOfOrigin: null, hallmarking: null)]);
        var sut = CreateHandler(out var service, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await service.Received(1).ReceiveShipmentLineAsync(
            Arg.Any<string>(), Arg.Any<string>(), "UNKNOWN", "NON", Arg.Any<int>(), Arg.Any<bool>(), Arg.Any<State>(), Arg.Any<Status>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.6 publishes the OMS delta when eligible, IsB2CChanged is true, and the feature flag is enabled")]
    public async Task HandleAsync_OmsDeltaEligibleB2CChangedAndFlagEnabled_PublishesOmsDelta()
    {
        var target = CreateEvent(warehouseCode: "", locationTo: new InventoryEventLocation(FulfilmentLocationIds.Caecom, InventoryEventLocationType.Warehouse));
        var sut = CreateHandler(
            out _, out var deltaTowardsOmsPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsOms = true },
            serviceResult: new GoodsInTransitReceiptResult { IsB2CChanged = true, DeltaTowardsOms = 5 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.Received(1).PublishAsync(
            "SKU-1", FulfilmentLocationIds.Caecom, InventoryEventLocationType.Warehouse.ToString(), "DK", "585", 5, "12345", Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.6 skips the OMS delta publish when the feature flag is disabled")]
    public async Task HandleAsync_OmsDeltaEligibleButFlagDisabled_SkipsOmsDeltaPublish()
    {
        var target = CreateEvent(warehouseCode: "", locationTo: new InventoryEventLocation(FulfilmentLocationIds.Caecom, InventoryEventLocationType.Warehouse));
        var sut = CreateHandler(
            out _, out var deltaTowardsOmsPublisher, out _,
            featureFlags: new FeatureFlagsOptions(),
            serviceResult: new GoodsInTransitReceiptResult { IsB2CChanged = true, DeltaTowardsOms = 5 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync §3.6 skips the OMS delta publish when a warehouse code makes the shipment ineligible, even with IsB2CChanged")]
    public async Task HandleAsync_WarehouseCodePresent_SkipsOmsDeltaPublishRegardlessOfB2CChanged()
    {
        var target = CreateEvent(warehouseCode: "EDC", locationTo: new InventoryEventLocation(FulfilmentLocationIds.Caecom, InventoryEventLocationType.Warehouse));
        var sut = CreateHandler(
            out _, out var deltaTowardsOmsPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsOms = true },
            serviceResult: new GoodsInTransitReceiptResult { IsB2CChanged = true, DeltaTowardsOms = 5 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync §7.3 publishes exactly one order-tracking request covering every shipment line after all lines are applied")]
    public async Task HandleAsync_MultipleShipmentLines_PublishesOneOrderTrackingRequestWithAllLines()
    {
        var target = CreateEvent(
            packingSlipId: "PS12345",
            shipmentLines:
            [
                CreateLine(productId: "SKU-1", quantity: 5, countryOfOrigin: "DK", hallmarking: "585"),
                CreateLine(productId: "SKU-2", quantity: 3, countryOfOrigin: "TH", hallmarking: "NON"),
            ]);
        var sut = CreateHandler(out _, out _, out var orderTrackingPublisher);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.Received(1).PublishAsync(
            Arg.Is<OrderTrackingRelayRequest>(r =>
                r.ReferenceId == "12345" &&
                r.OrderId == "12345" &&
                r.Channel == InventoryEventChannel.IntercompanyDistribution.ToString() &&
                r.FulfilmentUnitId == "UNKNOWN" &&
                r.FulfilmentUnitType == InventoryEventLocationType.Warehouse.ToString() &&
                r.FunctionName == nameof(GoodsInTransitReceivedHandler) &&
                r.OrderStatus == OrderTrackingStatus.RECEIVED &&
                r.OrderType == OrderType.TRANSFER.ToString() &&
                r.Lines.Count == 2 &&
                r.Lines[0].ItemCode == "SKU-1" && r.Lines[0].CountryOfOrigin == "DK" && r.Lines[0].HallMarkType == "585" && r.Lines[0].Qty == 5 &&
                r.Lines[1].ItemCode == "SKU-2" && r.Lines[1].CountryOfOrigin == "TH" && r.Lines[1].HallMarkType == "NON" && r.Lines[1].Qty == 3),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.1 normalizes a PS-prefixed packing slip id before using it as the order-tracking reference id")]
    public async Task HandleAsync_PsPrefixedPackingSlipId_NormalizesBeforePublishingOrderTracking()
    {
        var target = CreateEvent(packingSlipId: "PS98765");
        var sut = CreateHandler(out _, out _, out var orderTrackingPublisher);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.Received(1).PublishAsync(
            Arg.Is<OrderTrackingRelayRequest>(r => r.ReferenceId == "98765" && r.OrderId == "98765"),
            Arg.Any<CancellationToken>());
    }
}
