using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Handlers;
using Microsoft.Extensions.Logging;
using NSubstitute;

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
        string? referenceId = "REF-1") => new(
        Channel: InventoryEventChannel.OwnOnline,
        Id: "state-1",
        ChangeDate: new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        Location: new InventoryEventLocation("WH-1", InventoryEventLocationType.Warehouse),
        Entity: "ORG-1",
        Type: type,
        FromState: new InventoryEventStateSnapshot(fromState, fromStatus),
        ToState: new InventoryEventStateSnapshot(toState, toStatus),
        ItemLines:
        [
            new InventoryEventItemLine(
                LineNum: "1",
                ProductId: "SKU-1",
                ItemName: "Item 1",
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
                IsBonded: null),
        ],
        ReferenceId: referenceId);

    private static InventoryStateChangedHandler CreateHandler(out IItemStockInventoryService itemStockInventoryService)
    {
        itemStockInventoryService = Substitute.For<IItemStockInventoryService>();

        return new InventoryStateChangedHandler(itemStockInventoryService, Substitute.For<ILogger<InventoryStateChangedHandler>>());
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
        var sut = CreateHandler(out var itemStockInventoryService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryService.Received(1).ApplyPickAsync(
            "WH-1", "SKU-1", "TH", "925", ItemStockPickChannel.B2B, 2, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync applies a B2C pick mutation for each item line on a PickedB2C transition")]
    public async Task HandleAsync_PickedB2CTransition_AppliesPickForEachItemLine()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            type: InventoryEventChangeType.PickedB2C);
        var sut = CreateHandler(out var itemStockInventoryService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryService.Received(1).ApplyPickAsync(
            "WH-1", "SKU-1", "TH", "925", ItemStockPickChannel.B2C, 2, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync applies an unpick mutation for each item line on a Dgp unpick transition")]
    public async Task HandleAsync_DgpUnpickTransition_AppliesUnpickForEachItemLine()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            InventoryEventStockState.Available, InventoryEventStockStatus.Held,
            type: InventoryEventChangeType.Dgp);
        var sut = CreateHandler(out var itemStockInventoryService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryService.Received(1).ApplyUnpickAsync(
            "WH-1", "SKU-1", "TH", "925", 2, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync skips the unpick mutation when the unpick transition's Type is not Dgp")]
    public async Task HandleAsync_UnpickTransitionWithNonDgpType_SkipsMutation()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            type: InventoryEventChangeType.PickedB2C);
        var sut = CreateHandler(out var itemStockInventoryService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryService.DidNotReceiveWithAnyArgs().ApplyUnpickAsync(
            default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync skips the pick mutation when the pick transition's Type is neither PickedB2B nor PickedB2C")]
    public async Task HandleAsync_PickTransitionWithUnsupportedType_SkipsMutation()
    {
        var target = CreateEvent(
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Prepared,
            type: InventoryEventChangeType.Dgp);
        var sut = CreateHandler(out var itemStockInventoryService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryService.DidNotReceiveWithAnyArgs().ApplyPickAsync(
            default!, default!, default!, default!, default, default, default);
    }

    [Fact(DisplayName = "HandleAsync completes without touching the service for an unrelated transition")]
    public async Task HandleAsync_UnrelatedTransition_CompletesSuccessfullyWithoutMutating()
    {
        var target = CreateEvent(
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);
        var sut = CreateHandler(out var itemStockInventoryService);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryService.DidNotReceiveWithAnyArgs().ApplyPickAsync(
            default!, default!, default!, default!, default, default, default);
        await itemStockInventoryService.DidNotReceiveWithAnyArgs().ApplyUnpickAsync(
            default!, default!, default!, default!, default, default);
    }
}
