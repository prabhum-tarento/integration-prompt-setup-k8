using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged.Mappers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using net.pandora.nexus.shared;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="InternalHallmarkingStatusChangedEventMapper"/> - the hand-written
/// mapping from the Avro-generated <see cref="net.pandora.nexus.@event.inventory.InternalHallmarkingStatusChanged"/>
/// to this consumer's own decoupled <see cref="InternalHallmarkingStatusChangedEvent"/> wire contract.
/// Focused on the two colliding <c>Status</c> enums this schema introduces (this event's own top-level
/// STARTED/PICKED/CHANGED/FINISHED vs. <c>inventoryState.status</c>'s PICKABLE/HELD/PREPARED/HALLMARKING/
/// ALLOCATED/INVOICED) and the singular (non-collection) item line.
/// </summary>
public class InternalHallmarkingStatusChangedEventMapperTests
{
    private static net.pandora.nexus.@event.inventory.InternalHallmarkingStatusChanged CreateSource(
        net.pandora.nexus.@event.inventory.Status status = net.pandora.nexus.@event.inventory.Status.STARTED) => new()
    {
        channel = Channel.OWN_ONLINE,
        status = status,
        id = "hallmark-1",
        changeDate = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        location = new Location { id = "WH-1", type = LocationType.WAREHOUSE },
        entity = "ORG-1",
        type = net.pandora.nexus.@object.inventory.InventoryChangeType.MQA,
        inventoryState = new net.pandora.nexus.@object.inventory.InventoryState
        {
            state = net.pandora.nexus.@object.inventory.State.AVAILABLE,
            status = net.pandora.nexus.@object.inventory.Status.HALLMARKING,
        },
        itemLine = new net.pandora.nexus.@object.inventory.HallmarkingItemLine
        {
            lineNum = "1",
            productId = "SKU-1",
            quantity = 4,
            countryOfOrigin = CountryCode.TH,
            hallmarkingFrom = "NON",
            hallmarkingTo = "925",
            reasonCode = "ALLOCATION",
        },
    };

    [Fact(DisplayName = "ToInternalHallmarkingStatusChangedEvent maps top-level scalar fields as-is")]
    public void ToInternalHallmarkingStatusChangedEvent_ScalarFields_MapsUnchanged()
    {
        var result = CreateSource().ToInternalHallmarkingStatusChangedEvent();

        Assert.Equal("hallmark-1", result.Id);
        Assert.Equal(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc), result.ChangeDate);
        Assert.Equal("ORG-1", result.Entity);
    }

    [Theory(DisplayName = "ToInternalHallmarkingStatusChangedEvent maps the event's own top-level Status by symbol name, distinct from inventoryState.status's enum")]
    [InlineData(net.pandora.nexus.@event.inventory.Status.UNKNOWN, Status.Unknown)]
    [InlineData(net.pandora.nexus.@event.inventory.Status.STARTED, Status.Started)]
    [InlineData(net.pandora.nexus.@event.inventory.Status.PICKED, Status.Picked)]
    [InlineData(net.pandora.nexus.@event.inventory.Status.CHANGED, Status.Changed)]
    [InlineData(net.pandora.nexus.@event.inventory.Status.FINISHED, Status.Finished)]
    public void ToInternalHallmarkingStatusChangedEvent_TopLevelStatus_MapsBySymbolNameNotOrdinal(
        net.pandora.nexus.@event.inventory.Status source, Status expected)
    {
        var result = CreateSource(source).ToInternalHallmarkingStatusChangedEvent();

        Assert.Equal(expected, result.Status);
    }

    [Fact(DisplayName = "ToInternalHallmarkingStatusChangedEvent maps inventoryState.status via the shared InventoryEventStockStatus enum, not this event's own Status")]
    public void ToInternalHallmarkingStatusChangedEvent_InventoryStateStatus_MapsToSharedStockStatusEnum()
    {
        var result = CreateSource().ToInternalHallmarkingStatusChangedEvent();

        Assert.Equal(InventoryEventStockStatus.Hallmarking, result.InventoryState.Status);
        Assert.Equal(InventoryEventStockState.Available, result.InventoryState.State);
    }

    [Fact(DisplayName = "ToInternalHallmarkingStatusChangedEvent maps channel, location, and change type via the shared InventoryStateChanged mappers")]
    public void ToInternalHallmarkingStatusChangedEvent_SharedShapes_MapsViaReusedMappers()
    {
        var result = CreateSource().ToInternalHallmarkingStatusChangedEvent();

        Assert.Equal(InventoryEventChannel.OwnOnline, result.Channel);
        Assert.Equal(InventoryEventChangeType.Mqa, result.Type);
        Assert.Equal("WH-1", result.Location.Id);
        Assert.Equal(InventoryEventLocationType.Warehouse, result.Location.Type);
    }

    [Fact(DisplayName = "ToInternalHallmarkingStatusChangedEvent maps the single itemLine, not a collection")]
    public void ToInternalHallmarkingStatusChangedEvent_ItemLine_MapsSingularLine()
    {
        var result = CreateSource().ToInternalHallmarkingStatusChangedEvent();

        Assert.Equal("1", result.ItemLine.LineNum);
        Assert.Equal("SKU-1", result.ItemLine.ProductId);
        Assert.Equal(4, result.ItemLine.Quantity);
        Assert.Equal("TH", result.ItemLine.CountryOfOrigin);
        Assert.Equal("NON", result.ItemLine.HallmarkingFrom);
        Assert.Equal("925", result.ItemLine.HallmarkingTo);
        Assert.Equal("ALLOCATION", result.ItemLine.ReasonCode);
    }
}
