using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Application.OrderTracking;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped.Handlers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="ConsolidatedOrderShippedHandler"/> - per-line B2B confirmation, OMS-delta/ICR
/// feature-flag gating, the DEECOMDC e-commerce engraving branch (match/mismatch/empty-CustomerId),
/// and the order-tracking grouping/eligibility/publish flow
/// (docs/events/b2b.sales.ConsolidatedOrderShipped.md §2/§3).
/// </summary>
public class ConsolidatedOrderShippedHandlerTests
{
    private const string WarehouseCode = "WH-1";
    private const string ParentOrderId = "parent-order-1";
    private const string ShipmentId = "shipment-1";

    private readonly IConsolidatedOrderShippedService consolidatedOrderShippedService = Substitute.For<IConsolidatedOrderShippedService>();
    private readonly IItemStockWarehouseInventoryService itemStockWarehouseInventoryService = Substitute.For<IItemStockWarehouseInventoryService>();
    private readonly IItemStockInventorySegmentationService itemStockInventorySegmentationService = Substitute.For<IItemStockInventorySegmentationService>();
    private readonly IOrderTrackingRepository orderTrackingRepository = Substitute.For<IOrderTrackingRepository>();
    private readonly IEcomCustomerRepository ecomCustomerRepository = Substitute.For<IEcomCustomerRepository>();
    private readonly IDeltaTowardsOmsPublisher deltaTowardsOmsPublisher = Substitute.For<IDeltaTowardsOmsPublisher>();
    private readonly IInventoryComparisonReportPublisher inventoryComparisonReportPublisher = Substitute.For<IInventoryComparisonReportPublisher>();
    private readonly IOrderTrackingPublisher orderTrackingPublisher = Substitute.For<IOrderTrackingPublisher>();
    private readonly IMessageArchiveWriter archiveWriter = Substitute.For<IMessageArchiveWriter>();

    public ConsolidatedOrderShippedHandlerTests()
    {
        consolidatedOrderShippedService
            .ConfirmAsync(Arg.Any<B2BOrderConfirmedRequest>(), Arg.Any<CancellationToken>())
            .Returns(new ItemStockInventoryDeltaResult());
        orderTrackingRepository.GetAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns((OrderTracking?)null);
    }

    private ConsolidatedOrderShippedHandler CreateHandler(
        bool enableDeltaTowardsOms = false, bool enableSnapshotForIcr = false)
    {
        var options = Substitute.For<IOptions<FeatureFlagsOptions>>();
        options.Value.Returns(new FeatureFlagsOptions
        {
            EnableDeltaTowardsOms = enableDeltaTowardsOms,
            EnableSnapshotForIcr = enableSnapshotForIcr,
        });

        return new ConsolidatedOrderShippedHandler(
            consolidatedOrderShippedService,
            itemStockWarehouseInventoryService,
            itemStockInventorySegmentationService,
            orderTrackingRepository,
            ecomCustomerRepository,
            deltaTowardsOmsPublisher,
            inventoryComparisonReportPublisher,
            orderTrackingPublisher,
            archiveWriter,
            options,
            Substitute.For<ILogger<ConsolidatedOrderShippedHandler>>());
    }

    private static ConsolidatedOrderShipmentLine CreateLine(
        string productId = "item-1", string orderId = "order-1", string pickingRouteId = "route-1",
        int quantity = 4, int? allocatedFromB2BBucketQuantity = 4) => new(
        LineNum: "1",
        PickingRouteId: pickingRouteId,
        OrderId: orderId,
        LotId: "lot-1",
        ProductId: productId,
        Quantity: quantity,
        Hallmarking: "925",
        AllocatedFromB2BBucketQuantity: allocatedFromB2BBucketQuantity,
        CountryOfOrigin: "TH");

    private static ConsolidatedOrderShippedEvent CreateEvent(
        string warehouseCode = WarehouseCode,
        ConfirmationType confirmationType = ConfirmationType.STANDARD,
        bool isExport = false,
        IReadOnlyList<ConsolidatedOrderShipmentLine>? lines = null) => new(
        Channel: InventoryEventChannel.OwnOnline,
        Market: "DK",
        ParentOrderId: ParentOrderId,
        Shipment: new ConsolidatedOrderShipment(
            Id: ShipmentId,
            WarehouseCode: warehouseCode,
            ConfirmationType: confirmationType,
            ShipDate: new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            PackingSlipId: "packing-slip-1",
            ShipmentLines: lines ?? [CreateLine()]),
        IsExport: isExport);

    // ---------- Per-line B2B confirmation (§3.1/§4.1) ----------

    [Fact(DisplayName = "HandleAsync confirms each shipment line via IConsolidatedOrderShippedService")]
    public async Task HandleAsync_MultipleLines_ConfirmsEachLine()
    {
        var target = CreateEvent(lines: [CreateLine(productId: "item-1"), CreateLine(productId: "item-2")]);
        var sut = CreateHandler();

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await consolidatedOrderShippedService.Received(1).ConfirmAsync(
            Arg.Is<B2BOrderConfirmedRequest>(r => r.ItemCode == "item-1" && r.FulfilmentCode == WarehouseCode), Arg.Any<CancellationToken>());
        await consolidatedOrderShippedService.Received(1).ConfirmAsync(
            Arg.Is<B2BOrderConfirmedRequest>(r => r.ItemCode == "item-2" && r.FulfilmentCode == WarehouseCode), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync applies item-level segmentation for every shipment line")]
    public async Task HandleAsync_EveryLine_AppliesSegmentation()
    {
        var target = CreateEvent(lines: [CreateLine(productId: "item-1", quantity: 4)]);
        var sut = CreateHandler();

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventorySegmentationService.Received(1).ApplySegmentationAsync(
            WarehouseCode, "item-1", "TH", "925", 4, true, Arg.Any<CancellationToken>());
    }

    // ---------- OMS delta gating (§3.1 step 7) ----------

    [Fact(DisplayName = "HandleAsync publishes the OMS delta when B2C changed and EnableDeltaTowardsOms is on")]
    public async Task HandleAsync_B2CChangedAndFlagEnabled_PublishesOmsDelta()
    {
        consolidatedOrderShippedService
            .ConfirmAsync(Arg.Any<B2BOrderConfirmedRequest>(), Arg.Any<CancellationToken>())
            .Returns(new ItemStockInventoryDeltaResult { IsB2CChanged = true, DeltaTowardsOms = 3 });
        var target = CreateEvent();
        var sut = CreateHandler(enableDeltaTowardsOms: true);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.Received(1).PublishAsync(
            "item-1", WarehouseCode, InventoryEventLocationType.Warehouse.ToString(), "TH", "925", 3, "1", Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync does not publish the OMS delta when EnableDeltaTowardsOms is off, even if B2C changed")]
    public async Task HandleAsync_B2CChangedButFlagDisabled_DoesNotPublishOmsDelta()
    {
        consolidatedOrderShippedService
            .ConfirmAsync(Arg.Any<B2BOrderConfirmedRequest>(), Arg.Any<CancellationToken>())
            .Returns(new ItemStockInventoryDeltaResult { IsB2CChanged = true, DeltaTowardsOms = 3 });
        var target = CreateEvent();
        var sut = CreateHandler(enableDeltaTowardsOms: false);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync does not publish the OMS delta when B2C did not change, even if the flag is on")]
    public async Task HandleAsync_B2CNotChanged_DoesNotPublishOmsDelta()
    {
        var target = CreateEvent();
        var sut = CreateHandler(enableDeltaTowardsOms: true);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default, default!, default);
    }

    // ---------- ICR snapshot gating (§5) ----------

    [Fact(DisplayName = "HandleAsync publishes the ICR snapshot for every line when EnableSnapshotForIcr is on")]
    public async Task HandleAsync_IcrFlagEnabled_PublishesSnapshot()
    {
        var target = CreateEvent();
        var sut = CreateHandler(enableSnapshotForIcr: true);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryComparisonReportPublisher.Received(1).PublishAsync(
            WarehouseCode, "item-1", "925", "TH", true, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync does not publish the ICR snapshot when EnableSnapshotForIcr is off")]
    public async Task HandleAsync_IcrFlagDisabled_DoesNotPublishSnapshot()
    {
        var target = CreateEvent();
        var sut = CreateHandler(enableSnapshotForIcr: false);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryComparisonReportPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default, default);
    }

    // ---------- DEECOMDC e-commerce engraving branch (§3.3) ----------

    [Fact(DisplayName = "HandleAsync skips engraving when the resolved OrderTracking record's CustomerId is empty")]
    public async Task HandleAsync_EmptyCustomerId_SkipsEngraving()
    {
        orderTrackingRepository.GetAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns((OrderTracking?)null);
        var target = CreateEvent();
        var sut = CreateHandler();

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockWarehouseInventoryService.DidNotReceiveWithAnyArgs().ApplyShipmentAsync(
            default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync skips engraving when the resolved CustomerId does not match the Ecom reference data")]
    public async Task HandleAsync_CustomerIdMismatch_SkipsEngraving()
    {
        var tracking = OrderTracking.Rehydrate("tracking-1", "tracking-1", ParentOrderId, "OTHER-CUSTOMER", ShipmentId, "SHIPPED");
        orderTrackingRepository.GetAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(tracking);
        var ecomCustomer = EcomCustomer.Rehydrate("ecom-1", "ecom-1", WarehouseCode, ["DEECOMDC"], "TDC-CUST");
        ecomCustomerRepository.GetAsync(WarehouseCode, Arg.Any<CancellationToken>()).Returns(ecomCustomer);
        var target = CreateEvent();
        var sut = CreateHandler();

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockWarehouseInventoryService.DidNotReceiveWithAnyArgs().ApplyShipmentAsync(
            default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync applies the engraving shipment for every line when the resolved CustomerId matches")]
    public async Task HandleAsync_CustomerIdMatch_AppliesEngravingForEveryLine()
    {
        var tracking = OrderTracking.Rehydrate("tracking-1", "tracking-1", ParentOrderId, "DEECOMDC", ShipmentId, "SHIPPED");
        orderTrackingRepository.GetAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(tracking);
        var ecomCustomer = EcomCustomer.Rehydrate("ecom-1", "ecom-1", WarehouseCode, ["DEECOMDC"], "TDC-CUST");
        ecomCustomerRepository.GetAsync(WarehouseCode, Arg.Any<CancellationToken>()).Returns(ecomCustomer);
        var target = CreateEvent(lines: [CreateLine(productId: "item-1", quantity: 4)]);
        var sut = CreateHandler();

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockWarehouseInventoryService.Received(1).ApplyShipmentAsync(
            WarehouseCode, "item-1", 4, Arg.Any<CancellationToken>());
    }

    // ---------- Order-tracking publish (§3.2) ----------

    [Fact(DisplayName = "HandleAsync publishes one order-tracking request per resolved group key")]
    public async Task HandleAsync_MultipleGroupKeys_PublishesOnePerGroup()
    {
        var target = CreateEvent(lines:
        [
            CreateLine(productId: "item-1", orderId: "order-a"),
            CreateLine(productId: "item-2", orderId: "order-b"),
        ]);
        var sut = CreateHandler();

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.Received(1).PublishAsync(
            Arg.Is<OrderTrackingRelayRequest>(r => r.OrderId == "order-a"), Arg.Any<CancellationToken>());
        await orderTrackingPublisher.Received(1).PublishAsync(
            Arg.Is<OrderTrackingRelayRequest>(r => r.OrderId == "order-b"), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync builds the order-tracking request with the expected B2B_CONSOLIDATED_ORDER_SHIPPED fields")]
    public async Task HandleAsync_OrderTrackingRequest_CarriesExpectedFields()
    {
        var target = CreateEvent(confirmationType: ConfirmationType.STANDARD, isExport: true);
        var sut = CreateHandler();

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.Received(1).PublishAsync(
            Arg.Is<OrderTrackingRelayRequest>(r =>
                r.ReferenceId == ParentOrderId &&
                r.Type == "B2B_CONSOLIDATED_ORDER_SHIPPED" &&
                r.PackingSlipId == "packing-slip-1" &&
                r.ShipmentId == ShipmentId &&
                r.Market == "DK" &&
                r.IsExport == true &&
                r.OrderStatus == OrderTrackingStatus.SHIPPED),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync skips the order-tracking publish for a preliminary confirmation that is not an export")]
    public async Task HandleAsync_PreliminaryNonExport_SkipsOrderTrackingPublish()
    {
        var target = CreateEvent(confirmationType: ConfirmationType.PRELIMINARY, isExport: false);
        var sut = CreateHandler();

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.DidNotReceiveWithAnyArgs().PublishAsync(default!, default);
    }

    [Fact(DisplayName = "HandleAsync excludes zero-quantity lines from the order-tracking groups")]
    public async Task HandleAsync_ZeroQuantityLine_ExcludedFromOrderTrackingGroups()
    {
        var target = CreateEvent(lines: [CreateLine(productId: "item-1", quantity: 0, orderId: "order-only")]);
        var sut = CreateHandler();

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await orderTrackingPublisher.DidNotReceiveWithAnyArgs().PublishAsync(default!, default);
    }

    // ---------- Archiving (best-effort) ----------

    [Fact(DisplayName = "HandleAsync enqueues a before and after archive entry")]
    public async Task HandleAsync_EveryCall_EnqueuesBeforeAndAfterArchiveEntries()
    {
        var target = CreateEvent();
        var sut = CreateHandler();

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        archiveWriter.Received(2).Enqueue(Arg.Any<MessageArchive>());
    }
}
