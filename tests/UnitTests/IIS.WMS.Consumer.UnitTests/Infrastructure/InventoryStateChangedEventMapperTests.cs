using IIS.WMS.Consumer.Infrastructure.Messaging;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using net.pandora.nexus.@event.inventory;
using net.pandora.nexus.shared;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="InventoryStateChangedEventMapper"/> - the hand-written mapping
/// from the Avro-generated <see cref="InventoryStateChanged"/> to this consumer's own decoupled
/// <see cref="InventoryStateChangedEvent"/> wire contract.
/// </summary>
public class InventoryStateChangedEventMapperTests
{
    private static InventoryStateChanged CreateSource() => new()
    {
        channel = Channel.OWN_ONLINE,
        id = "state-1",
        changeDate = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        location = new Location { id = "WH-1", type = LocationType.WAREHOUSE },
        entity = "ORG-1",
        type = net.pandora.nexus.@object.inventory.InventoryChangeType.MQA,
        fromState = new net.pandora.nexus.@object.inventory.InventoryState
        {
            state = net.pandora.nexus.@object.inventory.State.BLOCKED,
            status = net.pandora.nexus.@object.inventory.Status.HELD,
        },
        toState = new net.pandora.nexus.@object.inventory.InventoryState
        {
            state = net.pandora.nexus.@object.inventory.State.AVAILABLE,
            status = net.pandora.nexus.@object.inventory.Status.PICKABLE,
        },
        itemLines =
        [
            new net.pandora.nexus.@object.inventory.ItemLine
            {
                lineNum = "1",
                productId = "SKU-1",
                itemName = "Ring",
                quantity = 2,
                units = "EA",
                countryOfOrigin = CountryCode.TH,
                hallmarking = "NON",
                netWeight = new Weight { unit = WeightUnit.GRAM, quantity = 5 },
                tareWeight = null,
                unitPrice = new Money { currencyCode = CurrencyCode.USD, units = 19, nanos = 500_000_000 },
                commodityCode = "7113",
                itemCategoryLocalized = null,
                itemMaterialNameLocalized = null,
                inventoryRegistrationId = null,
                customsRegistrationLineNum = null,
                isBonded = true,
            },
        ],
        referenceId = "REF-1",
    };

    [Fact(DisplayName = "ToInventoryStateChangedEvent maps top-level scalar fields as-is")]
    public void ToInventoryStateChangedEvent_ScalarFields_MapsUnchanged()
    {
        var result = CreateSource().ToInventoryStateChangedEvent();

        Assert.Equal("state-1", result.Id);
        Assert.Equal(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc), result.ChangeDate);
        Assert.Equal("ORG-1", result.Entity);
        Assert.Equal("REF-1", result.ReferenceId);
    }

    [Fact(DisplayName = "ToInventoryStateChangedEvent maps enums by symbol name, not ordinal")]
    public void ToInventoryStateChangedEvent_Enums_MapsBySymbolName()
    {
        var result = CreateSource().ToInventoryStateChangedEvent();

        Assert.Equal(InventoryEventChannel.OwnOnline, result.Channel);
        Assert.Equal(InventoryEventChangeType.Mqa, result.Type);
        Assert.Equal(InventoryEventLocationType.Warehouse, result.Location.Type);
        Assert.Equal(InventoryEventStockState.Blocked, result.FromState.State);
        Assert.Equal(InventoryEventStockStatus.Held, result.FromState.Status);
        Assert.Equal(InventoryEventStockState.Available, result.ToState.State);
        Assert.Equal(InventoryEventStockStatus.Pickable, result.ToState.Status);
    }

    [Fact(DisplayName = "ToInventoryStateChangedEvent maps location id unchanged")]
    public void ToInventoryStateChangedEvent_Location_MapsId()
    {
        var result = CreateSource().ToInventoryStateChangedEvent();

        Assert.Equal("WH-1", result.Location.Id);
    }

    [Fact(DisplayName = "ToInventoryStateChangedEvent maps item lines, including nested weight/money and null tareWeight")]
    public void ToInventoryStateChangedEvent_ItemLines_MapsNestedFields()
    {
        var result = CreateSource().ToInventoryStateChangedEvent();

        var line = Assert.Single(result.ItemLines);
        Assert.Equal("1", line.LineNum);
        Assert.Equal("SKU-1", line.ProductId);
        Assert.Equal(2, line.Quantity);
        Assert.Equal("TH", line.CountryOfOrigin);
        Assert.True(line.IsBonded);
        Assert.Null(line.TareWeight);

        Assert.NotNull(line.NetWeight);
        Assert.Equal(InventoryEventWeightUnit.Gram, line.NetWeight!.Unit);
        Assert.Equal(5, line.NetWeight.Quantity);

        Assert.NotNull(line.UnitPrice);
        Assert.Equal("USD", line.UnitPrice!.CurrencyCode);
        Assert.Equal(19.5m, line.UnitPrice.Amount);
    }
}
