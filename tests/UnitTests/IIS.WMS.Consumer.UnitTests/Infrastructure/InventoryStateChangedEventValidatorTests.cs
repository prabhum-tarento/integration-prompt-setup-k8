using IIS.WMS.Consumer.Infrastructure.Messaging;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.Validators;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Business-rule tests for <see cref="InventoryStateChangedEventValidator"/> - ported from the
/// upstream Reflex facade's <c>InventoryStateChangedKafkaHandler.CanProcessMessage</c> (see that
/// class's remarks for the mapping between the two).
/// </summary>
public class InventoryStateChangedEventValidatorTests
{
    private static InventoryStateChangedEvent CreateEvent(
        string locationId,
        InventoryEventStockState fromState,
        InventoryEventStockStatus fromStatus,
        InventoryEventStockState toState,
        InventoryEventStockStatus toStatus) => new(
        Channel: InventoryEventChannel.OwnOnline,
        Id: "state-1",
        ChangeDate: new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        Location: new InventoryEventLocation(locationId, InventoryEventLocationType.Warehouse),
        Entity: "ORG-1",
        Type: InventoryEventChangeType.Mqa,
        FromState: new InventoryEventStateSnapshot(fromState, fromStatus),
        ToState: new InventoryEventStateSnapshot(toState, toStatus),
        ItemLines: [],
        ReferenceId: "REF-1");

    [Fact(DisplayName = "CanProcess returns true for an ordinary transition at a non-EDC/TDC/ADC location")]
    public void CanProcess_OrdinaryTransitionAtOtherLocation_ReturnsTrue()
    {
        var target = CreateEvent(
            "WH-1",
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);

        Assert.True(InventoryStateChangedEventValidator.CanProcess(target));
    }

    [Fact(DisplayName = "CanProcess returns false when From and To states/statuses are identical")]
    public void CanProcess_NoOpTransition_ReturnsFalse()
    {
        var target = CreateEvent(
            "WH-1",
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable);

        Assert.False(InventoryStateChangedEventValidator.CanProcess(target));
    }

    [Fact(DisplayName = "CanProcess returns true at EDC when the From state is Available")]
    public void CanProcess_EdcLocationFromStateAvailable_ReturnsTrue()
    {
        var target = CreateEvent(
            FulfilmentLocationIds.Edc,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable);

        Assert.True(InventoryStateChangedEventValidator.CanProcess(target));
    }

    [Fact(DisplayName = "CanProcess returns true at EDC when the To state is Available")]
    public void CanProcess_EdcLocationToStateAvailable_ReturnsTrue()
    {
        var target = CreateEvent(
            FulfilmentLocationIds.Edc,
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);

        Assert.True(InventoryStateChangedEventValidator.CanProcess(target));
    }

    [Fact(DisplayName = "CanProcess returns false at EDC when neither side is Available")]
    public void CanProcess_EdcLocationNeitherStateAvailable_ReturnsFalse()
    {
        var target = CreateEvent(
            FulfilmentLocationIds.Edc,
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Inspection, InventoryEventStockStatus.Prepared);

        Assert.False(InventoryStateChangedEventValidator.CanProcess(target));
    }

    [Theory(DisplayName = "CanProcess returns true for a Held-to-Held transition at TDC/ADC")]
    [InlineData("Tdc")]
    [InlineData("Adc")]
    public void CanProcess_BothHeldAtTdcOrAdc_ReturnsTrue(string locationField)
    {
        var locationId = locationField == "Tdc" ? FulfilmentLocationIds.Tdc : FulfilmentLocationIds.Adc;
        var target = CreateEvent(
            locationId,
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            InventoryEventStockState.Available, InventoryEventStockStatus.Held);

        Assert.True(InventoryStateChangedEventValidator.CanProcess(target));
    }

    [Fact(DisplayName = "CanProcess returns false for a Held-to-Held transition outside TDC/ADC")]
    public void CanProcess_BothHeldAtOtherLocation_ReturnsFalse()
    {
        var target = CreateEvent(
            "WH-1",
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Held,
            InventoryEventStockState.Available, InventoryEventStockStatus.Held);

        Assert.False(InventoryStateChangedEventValidator.CanProcess(target));
    }

    [Fact(DisplayName = "CanProcess returns false when the From status is Hallmarking")]
    public void CanProcess_FromStatusHallmarking_ReturnsFalse()
    {
        var target = CreateEvent(
            "WH-1",
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Hallmarking,
            InventoryEventStockState.Available, InventoryEventStockStatus.Pickable);

        Assert.False(InventoryStateChangedEventValidator.CanProcess(target));
    }

    [Fact(DisplayName = "CanProcess returns false when the To status is Hallmarking")]
    public void CanProcess_ToStatusHallmarking_ReturnsFalse()
    {
        var target = CreateEvent(
            "WH-1",
            InventoryEventStockState.Blocked, InventoryEventStockStatus.Pickable,
            InventoryEventStockState.Available, InventoryEventStockStatus.Hallmarking);

        Assert.False(InventoryStateChangedEventValidator.CanProcess(target));
    }
}
