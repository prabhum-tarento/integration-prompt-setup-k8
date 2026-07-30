using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Handlers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using DomainEnums = IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="InventoryStateTransitionRules"/> and <see cref="InventoryStateChangedHandler"/> -
/// ported from the upstream Reflex facade's <c>InventoryStateChangedQueueTrigger</c>
/// isPickEvent/isUnpickEvent detection and its <c>InventoryPickEventHandler</c>/
/// <c>InventoryUnpickEventHandler</c> dispatch (see docs/InventoryStateChanged-OrderTracking-Relay.md).
/// </summary>
public class InventoryStateChangedHandlerTests
{
    private static InventoryStateChangedEvent CreateEvent(
        InventoryEventStockState fromState,
        InventoryEventStockStatus fromStatus,
        InventoryEventStockState toState,
        InventoryEventStockStatus toStatus,
        InventoryEventChangeType type = InventoryEventChangeType.PickedB2C,
        string? referenceId = "REF-1",
        string locationId = "WH-1",
        InventoryEventLocationType locationType = InventoryEventLocationType.Warehouse,
        int itemLineCount = 1) => new(
        Channel: InventoryEventChannel.OwnOnline,
        Id: "state-1",
        ChangeDate: new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        Location: new InventoryEventLocation(locationId, locationType),
        Entity: "ORG-1",
        Type: type,
        FromState: new InventoryEventStateSnapshot(fromState, fromStatus),
        ToState: new InventoryEventStateSnapshot(toState, toStatus),
        ItemLines:
        [
            .. Enumerable.Range(1, itemLineCount).Select(i => new InventoryEventItemLine(
                LineNum: i.ToString(),
                ProductId: $"SKU-{i}",
                ItemName: $"Item {i}",
                Quantity: 2,
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
        ],
        ReferenceId: referenceId);

    private static InventoryStateChangedHandler CreateHandler(out IItemStockInventoryExtensionService itemStockInventoryExtensionService)
    {
        itemStockInventoryExtensionService = Substitute.For<IItemStockInventoryExtensionService>();

        var noDelta = new ItemStockInventoryDeltaResult { IsB2CChanged = false, DeltaTowardsOms = 0 };
        itemStockInventoryExtensionService.ApplyPickB2BWithExtensionAsync(
            default!, default!, default!, default!, default, default).ReturnsForAnyArgs(noDelta);
        itemStockInventoryExtensionService.ApplyPickB2CWithExtensionAsync(
            default!, default!, default!, default!, default, default).ReturnsForAnyArgs(noDelta);
        itemStockInventoryExtensionService.ApplyUnpickWithExtensionAsync(
            default!, default!, default!, default!, default, default).ReturnsForAnyArgs(noDelta);

        var segmentationService = Substitute.For<IItemStockInventorySegmentationService>();
        segmentationService.ApplySegmentationAsync(
            default!, default!, default!, default!, default, default, default).ReturnsForAnyArgs(noDelta);

        var featureFlagsOptions = Substitute.For<IOptions<FeatureFlagsOptions>>();
        featureFlagsOptions.Value.Returns(new FeatureFlagsOptions());

        var consumerOptions = Substitute.For<IOptions<InventoryStateChangedServiceBusConsumerOptions>>();
        consumerOptions.Value.Returns(new InventoryStateChangedServiceBusConsumerOptions());

        return new InventoryStateChangedHandler(
            itemStockInventoryExtensionService,
            segmentationService,
            Substitute.For<IItemStockInventoryExtendedSegmentationService>(),
            Substitute.For<IInventoryAdjustedOrMovedPublisher>(),
            Substitute.For<IDeltaTowardsOmsPublisher>(),
            Substitute.For<IInventoryComparisonReportPublisher>(),
            featureFlagsOptions,
            consumerOptions,
            Substitute.For<ILogger<InventoryStateChangedHandler>>());
    }

    private static InventoryStateChangedHandler CreateHandler(
        out IItemStockInventorySegmentationService segmentationService,
        out IItemStockInventoryExtendedSegmentationService extendedSegmentationService,
        out IInventoryAdjustedOrMovedPublisher inventoryAdjustedOrMovedPublisher,
        out IDeltaTowardsOmsPublisher deltaTowardsOmsPublisher,
        out IInventoryComparisonReportPublisher inventoryComparisonReportPublisher,
        FeatureFlagsOptions? featureFlags = null,
        ItemStockInventoryDeltaResult? segmentationResult = null)
    {
        var itemStockInventoryExtensionService = Substitute.For<IItemStockInventoryExtensionService>();
        var noDelta = new ItemStockInventoryDeltaResult { IsB2CChanged = false, DeltaTowardsOms = 0 };
        itemStockInventoryExtensionService.ApplyPickB2BWithExtensionAsync(
            default!, default!, default!, default!, default, default).ReturnsForAnyArgs(noDelta);
        itemStockInventoryExtensionService.ApplyPickB2CWithExtensionAsync(
            default!, default!, default!, default!, default, default).ReturnsForAnyArgs(noDelta);
        itemStockInventoryExtensionService.ApplyUnpickWithExtensionAsync(
            default!, default!, default!, default!, default, default).ReturnsForAnyArgs(noDelta);

        segmentationService = Substitute.For<IItemStockInventorySegmentationService>();
        segmentationService.ApplySegmentationAsync(
            default!, default!, default!, default!, default, default, default).ReturnsForAnyArgs(segmentationResult ?? noDelta);

        extendedSegmentationService = Substitute.For<IItemStockInventoryExtendedSegmentationService>();
        inventoryAdjustedOrMovedPublisher = Substitute.For<IInventoryAdjustedOrMovedPublisher>();
        deltaTowardsOmsPublisher = Substitute.For<IDeltaTowardsOmsPublisher>();
        inventoryComparisonReportPublisher = Substitute.For<IInventoryComparisonReportPublisher>();

        var featureFlagsOptions = Substitute.For<IOptions<FeatureFlagsOptions>>();
        featureFlagsOptions.Value.Returns(featureFlags ?? new FeatureFlagsOptions());

        var consumerOptions = Substitute.For<IOptions<InventoryStateChangedServiceBusConsumerOptions>>();
        consumerOptions.Value.Returns(new InventoryStateChangedServiceBusConsumerOptions());

        return new InventoryStateChangedHandler(
            itemStockInventoryExtensionService,
            segmentationService,
            extendedSegmentationService,
            inventoryAdjustedOrMovedPublisher,
            deltaTowardsOmsPublisher,
            inventoryComparisonReportPublisher,
            featureFlagsOptions,
            consumerOptions,
            Substitute.For<ILogger<InventoryStateChangedHandler>>());
    }

    [Fact(DisplayName = "IsPickableToPrepared returns true for Available/Pickable to Available/Prepared")]
    public void IsPickableToPrepared_MatchingTransition_ReturnsTrue()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared);

        Assert.True(InventoryStateTransitionRules.IsPickableToPrepared(target));
        Assert.False(InventoryStateTransitionRules.IsUnpickTransition(target));
    }

    [Fact(DisplayName = "IsUnpickTransition returns true for Available/Prepared to Available/Held")]
    public void IsUnpickTransition_PreparedToHeld_ReturnsTrue()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            InventoryEventStockState.Available, InventoryEventStockStatus.Held);

        Assert.True(InventoryStateTransitionRules.IsUnpickTransition(target));
        Assert.False(InventoryStateTransitionRules.IsPickableToPrepared(target));
    }

    [Fact(DisplayName = "IsUnpickTransition returns true for Available/Prepared to Available/Pickable")]
    public void IsUnpickTransition_PreparedToPickable_ReturnsTrue()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);

        Assert.True(InventoryStateTransitionRules.IsUnpickTransition(target));
        Assert.False(InventoryStateTransitionRules.IsPickableToPrepared(target));
    }

    [Fact(DisplayName = "IsPickableToPrepared returns false for an unrelated transition")]
    public void IsPickableToPrepared_UnrelatedTransition_ReturnsFalse()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared);

        Assert.False(InventoryStateTransitionRules.IsPickableToPrepared(target));
    }

    [Fact(DisplayName = "IsUnpickTransition returns false for an unrelated transition")]
    public void IsUnpickTransition_UnrelatedTransition_ReturnsFalse()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held);

        Assert.False(InventoryStateTransitionRules.IsUnpickTransition(target));
    }

    [Fact(DisplayName = "HandleAsync applies a B2B pick mutation for each item line on a PickedB2B transition")]
    public async Task HandleAsync_PickedB2BTransition_AppliesPickForEachItemLine()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            type: InventoryEventChangeType.PickedB2B);
        var sut = CreateHandler(out var itemStockInventoryExtensionService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryExtensionService.Received(1).ApplyPickB2BWithExtensionAsync(
            "WH-1", "SKU-1", "TH", "925", 2, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync applies a B2C pick mutation for each item line on a PickedB2C transition")]
    public async Task HandleAsync_PickedB2CTransition_AppliesPickForEachItemLine()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            type: InventoryEventChangeType.PickedB2C);
        var sut = CreateHandler(out var itemStockInventoryExtensionService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryExtensionService.Received(1).ApplyPickB2CWithExtensionAsync(
            "WH-1", "SKU-1", "TH", "925", 2, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync applies an unpick mutation for each item line on a Dgp unpick transition")]
    public async Task HandleAsync_DgpUnpickTransition_AppliesUnpickForEachItemLine()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            InventoryEventStockState.Available, InventoryEventStockStatus.Held,
            type: InventoryEventChangeType.Dgp);
        var sut = CreateHandler(out var itemStockInventoryExtensionService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryExtensionService.Received(1).ApplyUnpickWithExtensionAsync(
            "WH-1", "SKU-1", "TH", "925", 2, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync skips the unpick mutation when the unpick transition's Type is not Dgp")]
    public async Task HandleAsync_UnpickTransitionWithNonDgpType_SkipsMutation()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            type: InventoryEventChangeType.PickedB2C);
        var sut = CreateHandler(out var itemStockInventoryExtensionService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryExtensionService.DidNotReceiveWithAnyArgs().ApplyUnpickWithExtensionAsync(
            default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync skips the pick mutation when the pick transition's Type is neither PickedB2B nor PickedB2C")]
    public async Task HandleAsync_PickTransitionWithUnsupportedType_SkipsMutation()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            type: InventoryEventChangeType.Dgp);
        var sut = CreateHandler(out var itemStockInventoryExtensionService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryExtensionService.DidNotReceiveWithAnyArgs().ApplyPickB2BWithExtensionAsync(
            default!, default!, default!, default!, default, default);
        await itemStockInventoryExtensionService.DidNotReceiveWithAnyArgs().ApplyPickB2CWithExtensionAsync(
            default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync completes without touching the service for an unrelated transition")]
    public async Task HandleAsync_UnrelatedTransition_CompletesSuccessfullyWithoutMutating()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);
        var sut = CreateHandler(out var itemStockInventoryExtensionService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryExtensionService.DidNotReceiveWithAnyArgs().ApplyPickB2BWithExtensionAsync(
            default!, default!, default!, default!, default, default);
        await itemStockInventoryExtensionService.DidNotReceiveWithAnyArgs().ApplyPickB2CWithExtensionAsync(
            default!, default!, default!, default!, default, default);
        await itemStockInventoryExtensionService.DidNotReceiveWithAnyArgs().ApplyUnpickWithExtensionAsync(
            default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync §3.3 calls the segmentation service when either side of a non-pick/unpick transition is Available/Pickable")]
    public async Task HandleAsync_SegmentationTriggerOnGenericTransition_CallsSegmentationService()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);
        var sut = CreateHandler(
            out var segmentationService, out _, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await segmentationService.Received(1).ApplySegmentationAsync(
            "WH-1", "SKU-1", "TH", "925", Arg.Any<int>(), false, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.3 skips the segmentation service when neither side of a non-pick/unpick transition is Available/Pickable")]
    public async Task HandleAsync_NoSegmentationTriggerOnGenericTransition_SkipsSegmentationService()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            InventoryEventStockState.Inspection, InventoryEventStockStatus.Held);
        var sut = CreateHandler(
            out var segmentationService, out _, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await segmentationService.DidNotReceiveWithAnyArgs().ApplySegmentationAsync(
            default!, default!, default!, default!, default, default, default);
    }

    [Fact(DisplayName = "HandleAsync §3.5 always calls the extended segmentation service on a non-pick/unpick transition, regardless of the segmentation trigger")]
    public async Task HandleAsync_GenericTransition_AlwaysCallsExtendedSegmentationService()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            InventoryEventStockState.Inspection, InventoryEventStockStatus.Held);
        var sut = CreateHandler(
            out _, out var extendedSegmentationService, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await extendedSegmentationService.Received(1).ApplyAsync(
            "WH-1", "SKU-1", "925", "TH",
            DomainEnums.State.BLOCKED, DomainEnums.Status.HELD,
            DomainEnums.State.INSPECTION, DomainEnums.Status.HELD,
            Arg.Any<int?>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.6 publishes the adjusted/moved event when EnableDeltaTowardsSap is set and the location is neither EDC nor ADC")]
    public async Task HandleAsync_EnableDeltaTowardsSapNonEdcNonAdcLocation_PublishesAdjustedOrMoved()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            InventoryEventStockState.Inspection, InventoryEventStockStatus.Held,
            locationId: "WH-1");
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsSap = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.Received(1).PublishAsync(
            "OwnOnline", "state-1", Arg.Any<DateTime>(), "WH-1", "Warehouse", "ORG-1",
            DomainEnums.State.BLOCKED, DomainEnums.Status.HELD,
            DomainEnums.State.INSPECTION, DomainEnums.Status.HELD,
            "REF-1", Arg.Any<IReadOnlyList<InventoryAdjustedOrMovedLine>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.6 skips the adjusted/moved publish when EnableDeltaTowardsSap is set but the location is EDC")]
    public async Task HandleAsync_EnableDeltaTowardsSapEdcLocation_SkipsAdjustedOrMovedPublish()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            InventoryEventStockState.Inspection, InventoryEventStockStatus.Held,
            locationId: FulfilmentLocationIds.Edc);
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsSap = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default, default!, default!, default, default, default, default, default, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync §3.6 publishes the adjusted/moved event when EnableDeltaTowardsAx123Pl is set and the location is CAECOM")]
    public async Task HandleAsync_EnableDeltaTowardsAx123PlCaecomLocation_PublishesAdjustedOrMoved()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            InventoryEventStockState.Inspection, InventoryEventStockStatus.Held,
            locationId: FulfilmentLocationIds.Caecom,
            locationType: InventoryEventLocationType.ThirdPartyLogistics);
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsAx123Pl = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.Received(1).PublishAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTime>(), FulfilmentLocationIds.Caecom, Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(), Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(),
            Arg.Any<string?>(), Arg.Any<IReadOnlyList<InventoryAdjustedOrMovedLine>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.6 publishes the adjusted/moved event when EnableAdcDeltaTowardsAx12 is set and the location is ADC")]
    public async Task HandleAsync_EnableAdcDeltaTowardsAx12AdcLocation_PublishesAdjustedOrMoved()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            InventoryEventStockState.Inspection, InventoryEventStockStatus.Held,
            locationId: FulfilmentLocationIds.Adc);
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableAdcDeltaTowardsAx12 = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.Received(1).PublishAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTime>(), FulfilmentLocationIds.Adc, Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(), Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(),
            Arg.Any<string?>(), Arg.Any<IReadOnlyList<InventoryAdjustedOrMovedLine>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.6 skips the adjusted/moved publish when EnableAdcDeltaTowardsAx12 is set but the location is not ADC")]
    public async Task HandleAsync_EnableAdcDeltaTowardsAx12NonAdcLocation_SkipsAdjustedOrMovedPublish()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            InventoryEventStockState.Inspection, InventoryEventStockStatus.Held,
            locationId: "WH-1");
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _,
            featureFlags: new FeatureFlagsOptions { EnableAdcDeltaTowardsAx12 = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default, default!, default!, default, default, default, default, default, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync §3.6 does not publish the adjusted/moved event when no relevant feature flag is enabled")]
    public async Task HandleAsync_NoFeatureFlagsEnabled_SkipsAdjustedOrMovedPublish()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            InventoryEventStockState.Inspection, InventoryEventStockStatus.Held,
            locationId: "WH-1");
        var sut = CreateHandler(
            out _, out _, out var inventoryAdjustedOrMovedPublisher, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryAdjustedOrMovedPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default, default!, default!, default, default, default, default, default, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync §3.7 publishes the OMS delta when segmentation reports IsB2CChanged and EnableDeltaTowardsOms is set for a non-3PL location")]
    public async Task HandleAsync_B2CChangedWithEnableDeltaTowardsOms_PublishesOmsDelta()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            locationId: "WH-1", locationType: InventoryEventLocationType.Warehouse);
        var sut = CreateHandler(
            out _, out _, out _, out var deltaTowardsOmsPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsOms = true },
            segmentationResult: new ItemStockInventoryDeltaResult { IsB2CChanged = true, DeltaTowardsOms = 260 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.Received(1).PublishAsync(
            "SKU-1", "WH-1", "Warehouse", "TH", "925", 260, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.7 publishes the OMS delta via the 3PL flag when the location type is ThirdPartyLogistics")]
    public async Task HandleAsync_B2CChangedThirdPartyLogisticsWithEnableDeltaTowardsOms3Pl_PublishesOmsDelta()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            locationId: FulfilmentLocationIds.Caecom, locationType: InventoryEventLocationType.ThirdPartyLogistics);
        var sut = CreateHandler(
            out _, out _, out _, out var deltaTowardsOmsPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsOms3Pl = true },
            segmentationResult: new ItemStockInventoryDeltaResult { IsB2CChanged = true, DeltaTowardsOms = 100 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.Received(1).PublishAsync(
            "SKU-1", FulfilmentLocationIds.Caecom, "ThirdPartyLogistics", "TH", "925", 100, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.7 does not publish the OMS delta when IsB2CChanged is false, even if EnableDeltaTowardsOms is set")]
    public async Task HandleAsync_B2CNotChanged_SkipsOmsDeltaPublish()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);
        var sut = CreateHandler(
            out _, out _, out _, out var deltaTowardsOmsPublisher, out _,
            featureFlags: new FeatureFlagsOptions { EnableDeltaTowardsOms = true },
            segmentationResult: new ItemStockInventoryDeltaResult { IsB2CChanged = false, DeltaTowardsOms = 0 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync §3.7 does not publish the OMS delta when IsB2CChanged is true but the relevant flag is disabled")]
    public async Task HandleAsync_B2CChangedButFlagDisabled_SkipsOmsDeltaPublish()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);
        var sut = CreateHandler(
            out _, out _, out _, out var deltaTowardsOmsPublisher, out _,
            featureFlags: new FeatureFlagsOptions(),
            segmentationResult: new ItemStockInventoryDeltaResult { IsB2CChanged = true, DeltaTowardsOms = 260 });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await deltaTowardsOmsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync §3.8 publishes the ICR snapshot per item line whenever EnableSnapshotForIcr is set, regardless of segmentation result")]
    public async Task HandleAsync_EnableSnapshotForIcr_PublishesIcrSnapshot()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            locationId: FulfilmentLocationIds.Caecom);
        var sut = CreateHandler(
            out _, out _, out _, out _, out var inventoryComparisonReportPublisher,
            featureFlags: new FeatureFlagsOptions { EnableSnapshotForIcr = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryComparisonReportPublisher.Received(1).PublishAsync(
            FulfilmentLocationIds.Caecom, "SKU-1", "925", "TH", true, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §3.8 does not publish the ICR snapshot when EnableSnapshotForIcr is disabled")]
    public async Task HandleAsync_SnapshotForIcrDisabled_SkipsIcrSnapshotPublish()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);
        var sut = CreateHandler(
            out _, out _, out _, out _, out var inventoryComparisonReportPublisher,
            featureFlags: new FeatureFlagsOptions());

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await inventoryComparisonReportPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync applies the pick mutation for every item line when a message has multiple item lines")]
    public async Task HandleAsync_MultipleItemLinesPickedB2CTransition_AppliesPickForEveryItemLine()
    {
        const int itemLineCount = 25;
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            type: InventoryEventChangeType.PickedB2C,
            itemLineCount: itemLineCount);
        var sut = CreateHandler(out var itemStockInventoryExtensionService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        for (var i = 1; i <= itemLineCount; i++)
        {
            await itemStockInventoryExtensionService.Received(1).ApplyPickB2CWithExtensionAsync(
                "WH-1", $"SKU-{i}", "TH", "925", 2, Arg.Any<CancellationToken>());
        }
    }

    [Fact(DisplayName = "HandleAsync still attempts every item line when one item line's mutation throws, then rethrows")]
    public async Task HandleAsync_OneOfManyItemLinesThrows_StillAttemptsEveryItemLineThenRethrows()
    {
        const int itemLineCount = 10;
        const int failingLine = 5;
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            type: InventoryEventChangeType.PickedB2C,
            itemLineCount: itemLineCount);
        var sut = CreateHandler(out var itemStockInventoryExtensionService);

        itemStockInventoryExtensionService.ApplyPickB2CWithExtensionAsync(
            "WH-1", $"SKU-{failingLine}", "TH", "925", 2, Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("simulated failure"));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => sut.HandleAsync(target, "corr-1", CancellationToken.None));

        for (var i = 1; i <= itemLineCount; i++)
        {
            await itemStockInventoryExtensionService.Received(1).ApplyPickB2CWithExtensionAsync(
                "WH-1", $"SKU-{i}", "TH", "925", 2, Arg.Any<CancellationToken>());
        }
    }

    [Fact(DisplayName = "HandleAsync prioritizes ConcurrencyException over other faults so RunProcessMessageAsync still maps the outcome to Abandoned")]
    public async Task HandleAsync_ConcurrencyExceptionAmongOtherFaults_RethrowsConcurrencyException()
    {
        const int itemLineCount = 10;
        const int concurrencyFailingLine = 3;
        const int otherFailingLine = 7;
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            type: InventoryEventChangeType.PickedB2C,
            itemLineCount: itemLineCount);
        var sut = CreateHandler(out var itemStockInventoryExtensionService);

        itemStockInventoryExtensionService.ApplyPickB2CWithExtensionAsync(
            "WH-1", $"SKU-{concurrencyFailingLine}", "TH", "925", 2, Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException($"SKU-{concurrencyFailingLine}", "etag-1"));
        itemStockInventoryExtensionService.ApplyPickB2CWithExtensionAsync(
            "WH-1", $"SKU-{otherFailingLine}", "TH", "925", 2, Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("simulated failure"));

        await Assert.ThrowsAsync<ConcurrencyException>(
            () => sut.HandleAsync(target, "corr-1", CancellationToken.None));
    }
}
