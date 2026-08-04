using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryAdjusted;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryAdjusted.Handlers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using DomainEnums = IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="InventoryAdjustedHandler"/> - ported from the upstream Reflex facade's
/// <c>InventoryAdjustedQueueTrigger</c> (see docs/events/inventory.InventoryAdjusted.md). Unlike
/// <see cref="InventoryStateChangedHandlerTests"/>'s sibling coverage, there is no pick/unpick
/// classification and no order-tracking publish to cover here - this event carries a single
/// <see cref="InventoryEventAdjustment.State"/> snapshot, and every line always runs the
/// segmentation/extended-segmentation branch.
/// </summary>
public class InventoryAdjustedHandlerTests
{
    private static InventoryAdjustedEvent CreateEvent(
        InventoryEventStockState state,
        InventoryEventStockStatus status,
        string locationId = "WH-1",
        InventoryEventLocationType locationType = InventoryEventLocationType.Warehouse,
        InventoryEventReasonCode reason = InventoryEventReasonCode.Adjustment,
        string? referenceId = "REF-1",
        int itemLineCount = 1,
        int quantity = 2) => new(
        Channel: InventoryEventChannel.OwnOnline,
        Adjustment: new InventoryEventAdjustment(
            ReferenceId: referenceId!,
            AdjustmentDate: new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            Entity: "ORG-1",
            Type: InventoryEventChangeType.Mav,
            State: new InventoryEventStateSnapshot(state, status),
            Location: new InventoryEventLocation(locationId, locationType),
            Reason: reason,
            AdjustmentLines:
            [
                .. Enumerable.Range(1, itemLineCount).Select(i => new InventoryEventItemLine(
                    LineNum: i.ToString(),
                    ProductId: $"SKU-{i}",
                    ItemName: $"Item {i}",
                    Quantity: quantity,
                    Units: "EA",
                    CountryOfOrigin: "TH",
                    Hallmarking: "925",
                    NetWeight: null,
                    TareWeight: null,
                    UnitPrice: null,
                    CommodityCode: null,
                    ItemCategoryLocalized: null,
                    ItemMaterialNameLocalized: null,
                    InventoryRegistrationId: null,
                    CustomsRegistrationLineNum: null,
                    IsBonded: null)),
            ]));

    private static InventoryAdjustedHandler CreateHandler(
        out IItemStockInventorySegmentationService segmentationService,
        out IItemStockInventoryExtendedSegmentationService extendedSegmentationService,
        out IInventoryAdjustedOrMovedPublisher inventoryAdjustedOrMovedPublisher,
        out IDeltaTowardsOmsPublisher deltaTowardsOmsPublisher,
        out IInventoryComparisonReportPublisher inventoryComparisonReportPublisher,
        FeatureFlagsOptions? featureFlags = null,
        ItemStockInventoryDeltaResult? segmentationResult = null)
    {
        var noDelta = new ItemStockInventoryDeltaResult { IsB2CChanged = false, DeltaTowardsOms = 0 };

        segmentationService = Substitute.For<IItemStockInventorySegmentationService>();
        segmentationService.ApplySegmentationAsync(
            default!, default!, default!, default!, default, default, default).ReturnsForAnyArgs(segmentationResult ?? noDelta);

        extendedSegmentationService = Substitute.For<IItemStockInventoryExtendedSegmentationService>();
        inventoryAdjustedOrMovedPublisher = Substitute.For<IInventoryAdjustedOrMovedPublisher>();
        deltaTowardsOmsPublisher = Substitute.For<IDeltaTowardsOmsPublisher>();
        inventoryComparisonReportPublisher = Substitute.For<IInventoryComparisonReportPublisher>();

        var featureFlagsOptions = Substitute.For<IOptions<FeatureFlagsOptions>>();
        featureFlagsOptions.Value.Returns(featureFlags ?? new FeatureFlagsOptions());

        var consumerOptions = Substitute.For<IOptions<InventoryAdjustedServiceBusConsumerOptions>>();
        consumerOptions.Value.Returns(new InventoryAdjustedServiceBusConsumerOptions());

        return new InventoryAdjustedHandler(
            segmentationService,
            extendedSegmentationService,
            inventoryAdjustedOrMovedPublisher,
            deltaTowardsOmsPublisher,
            inventoryComparisonReportPublisher,
            featureFlagsOptions,
            consumerOptions,
            Substitute.For<ILogger<InventoryAdjustedHandler>>());
    }

    [Fact(DisplayName = "HandleAsync §3.2 calls the segmentation service when the adjustment state is Available/Pickable")]
    public async Task HandleAsync_AvailablePickableState_CallsSegmentationService()
    {
        var target = CreateEvent(InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);
        var sut = CreateHandler(
            out var segmentationService, out _, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await segmentationService.Received(1).ApplySegmentationAsync(
            "WH-1", "SKU-1", "TH", "925", 2, false, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.2 skips the segmentation service when the adjustment state is not Available/Pickable")]
    public async Task HandleAsync_NonAvailablePickableState_SkipsSegmentationService()
    {
        var target = CreateEvent(InventoryEventStockState.Blocked, InventoryEventStockStatus.Held);
        var sut = CreateHandler(
            out var segmentationService, out _, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await segmentationService.DidNotReceiveWithAnyArgs().ApplySegmentationAsync(
            default!, default!, default!, default!, default, default, default);
    }

    [Fact(DisplayName = "HandleAsync §3.3 always calls the extended segmentation service with identical from/to state and status")]
    public async Task HandleAsync_AnyState_CallsExtendedSegmentationServiceWithSingleSnapshot()
    {
        var target = CreateEvent(InventoryEventStockState.Blocked, InventoryEventStockStatus.Held);
        var sut = CreateHandler(
            out _, out var extendedSegmentationService, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await extendedSegmentationService.Received(1).ApplyAsync(
            "WH-1", "SKU-1", "925", "TH",
            DomainEnums.State.BLOCKED, DomainEnums.Status.HELD,
            DomainEnums.State.BLOCKED, DomainEnums.Status.HELD,
            2, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.1 publishes the adjusted/moved event when EnableDeltaTowardsSap is set and the location is not ADC")]
    public async Task HandleAsync_EnableDeltaTowardsSapNonAdcLocation_PublishesAdjustedOrMoved()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            locationId: "WH-1");
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsSap = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.Received(1).PublishAsync(
            "OwnOnline", "REF-1", Arg.Any<DateTime>(), "WH-1", "Warehouse", "ORG-1",
            DomainEnums.State.BLOCKED, DomainEnums.Status.HELD,
            DomainEnums.State.BLOCKED, DomainEnums.Status.HELD,
            "REF-1", Arg.Any<IReadOnlyList<InventoryAdjustedOrMovedLine>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.1 publishes the adjusted/moved event when the location is CAECOM even with EnableAdcDeltaTowardsAx12 disabled - the doc-literal gate only restricts ADC, not every 3PL location")]
    public async Task HandleAsync_CaecomLocationWithAdcFlagDisabled_StillPublishesAdjustedOrMoved()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            locationId: FulfilmentLocationIds.Caecom, locationType: InventoryEventLocationType.ThirdPartyLogistics);
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsSap = true, EnableAdcDeltaTowardsAx12 = false });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.Received(1).PublishAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTime>(), FulfilmentLocationIds.Caecom, Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(), Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(),
            Arg.Any<string?>(), Arg.Any<IReadOnlyList<InventoryAdjustedOrMovedLine>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.1 skips the adjusted/moved publish when the location is ADC and EnableAdcDeltaTowardsAx12 is disabled")]
    public async Task HandleAsync_AdcLocationWithAdcFlagDisabled_SkipsAdjustedOrMovedPublish()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            locationId: FulfilmentLocationIds.Adc);
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsSap = true, EnableAdcDeltaTowardsAx12 = false });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default, default!, default!, default, default, default, default, default, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync §3.1 publishes the adjusted/moved event when the location is ADC and EnableAdcDeltaTowardsAx12 is enabled")]
    public async Task HandleAsync_AdcLocationWithAdcFlagEnabled_PublishesAdjustedOrMoved()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            locationId: FulfilmentLocationIds.Adc);
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsSap = true, EnableAdcDeltaTowardsAx12 = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.Received(1).PublishAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTime>(), FulfilmentLocationIds.Adc, Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(), Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(),
            Arg.Any<string?>(), Arg.Any<IReadOnlyList<InventoryAdjustedOrMovedLine>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.1 skips the adjusted/moved publish when EnableDeltaTowardsSap is disabled")]
    public async Task HandleAsync_EnableDeltaTowardsSapDisabled_SkipsAdjustedOrMovedPublish()
    {
        var target = CreateEvent(InventoryEventStockState.Blocked, InventoryEventStockStatus.Held);
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions());

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default, default!, default!, default, default, default, default, default, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync §3.1 derives ToState/ToStatus as UNKNOWN/UNKNOWN when the total adjustment quantity is negative")]
    public async Task HandleAsync_NegativeQuantity_DerivesUnknownToState()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            quantity: -3);
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsSap = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.Received(1).PublishAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTime>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>(),
            DomainEnums.State.AVAILABLE, DomainEnums.Status.PICKABLE,
            DomainEnums.State.UNKNOWN, DomainEnums.Status.UNKNOWN,
            Arg.Any<string?>(), Arg.Any<IReadOnlyList<InventoryAdjustedOrMovedLine>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.1 populates the Reason field on each adjusted/moved line from the adjustment's reason code")]
    public async Task HandleAsync_AnyAdjustment_PopulatesReasonOnLines()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            reason: InventoryEventReasonCode.Counting);
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsSap = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.Received(1).PublishAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTime>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(), Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(),
            Arg.Any<string?>(),
            Arg.Is<IReadOnlyList<InventoryAdjustedOrMovedLine>>(lines =>
                lines.Count == 1 && lines[0].Reason == nameof(InventoryEventReasonCode.Counting)),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.4 publishes the OMS delta when segmentation reports IsB2CChanged and EnableDeltaTowardsOms is set for a non-3PL location")]
    public async Task HandleAsync_B2CChangedWithEnableDeltaTowardsOms_PublishesOmsDelta()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            locationId: "WH-1", locationType: InventoryEventLocationType.Warehouse);
        var sut = CreateHandler(
            out _, out _, out _, out var deltaTowardsOmsPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsOms = true },
            segmentationResult: new ItemStockInventoryDeltaResult { IsB2CChanged = true, DeltaTowardsOms = 260 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.Received(1).PublishAsync(
            "SKU-1", "WH-1", "Warehouse", "TH", "925", 260, "REF-1", Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.4 publishes the OMS delta via the 3PL flag when the location type is ThirdPartyLogistics")]
    public async Task HandleAsync_B2CChangedThirdPartyLogisticsWithEnableDeltaTowardsOms3Pl_PublishesOmsDelta()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            locationId: FulfilmentLocationIds.Caecom, locationType: InventoryEventLocationType.ThirdPartyLogistics);
        var sut = CreateHandler(
            out _, out _, out _, out var deltaTowardsOmsPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsOms3Pl = true },
            segmentationResult: new ItemStockInventoryDeltaResult { IsB2CChanged = true, DeltaTowardsOms = 100 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.Received(1).PublishAsync(
            "SKU-1", FulfilmentLocationIds.Caecom, "ThirdPartyLogistics", "TH", "925", 100, "REF-1", Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.4 does not publish the OMS delta when IsB2CChanged is false, even if EnableDeltaTowardsOms is set")]
    public async Task HandleAsync_B2CNotChanged_SkipsOmsDeltaPublish()
    {
        var target = CreateEvent(InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);
        var sut = CreateHandler(
            out _, out _, out _, out var deltaTowardsOmsPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsOms = true },
            segmentationResult: new ItemStockInventoryDeltaResult { IsB2CChanged = false, DeltaTowardsOms = 0 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync §3.5 publishes the ICR snapshot per item line whenever EnableSnapshotForIcr is set, regardless of segmentation result")]
    public async Task HandleAsync_EnableSnapshotForIcr_PublishesIcrSnapshot()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            locationId: FulfilmentLocationIds.Caecom);
        var sut = CreateHandler(
            out _, out _, out _, out _, out var inventoryComparisonReportPublisher,
            featureFlags: new FeatureFlagsOptions { EnableSnapshotForIcr = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryComparisonReportPublisher.Received(1).PublishAsync(
            FulfilmentLocationIds.Caecom, "SKU-1", "925", "TH", true, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.5 does not publish the ICR snapshot when EnableSnapshotForIcr is disabled")]
    public async Task HandleAsync_SnapshotForIcrDisabled_SkipsIcrSnapshotPublish()
    {
        var target = CreateEvent(InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);
        var sut = CreateHandler(
            out _, out _, out _, out _, out var inventoryComparisonReportPublisher,
            featureFlags: new FeatureFlagsOptions());

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryComparisonReportPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync applies segmentation for every item line when a message has multiple item lines")]
    public async Task HandleAsync_MultipleItemLinesAvailablePickableState_AppliesSegmentationForEveryItemLine()
    {
        const int itemLineCount = 25;
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            itemLineCount: itemLineCount);
        var sut = CreateHandler(
            out var segmentationService, out _, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        for (var i = 1; i <= itemLineCount; i++)
        {
            await segmentationService.Received(1).ApplySegmentationAsync(
                "WH-1", $"SKU-{i}", "TH", "925", 2, false, Arg.Any<CancellationToken>());
        }
    }
}
