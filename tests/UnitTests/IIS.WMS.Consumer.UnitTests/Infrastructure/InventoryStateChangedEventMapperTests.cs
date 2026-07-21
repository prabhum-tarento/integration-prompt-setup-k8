using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
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

    [Theory(DisplayName = "ToChannel maps every Channel enum value by symbol name, including Unknown for the enum's own UNKNOWN member")]
    [InlineData(Channel.UNKNOWN, InventoryEventChannel.Unknown)]
    [InlineData(Channel.OWN_PHYSICAL, InventoryEventChannel.OwnPhysical)]
    [InlineData(Channel.OWN_ONLINE, InventoryEventChannel.OwnOnline)]
    [InlineData(Channel.PARTNER_ONLINE, InventoryEventChannel.PartnerOnline)]
    [InlineData(Channel.FRANCHISE, InventoryEventChannel.Franchise)]
    [InlineData(Channel.DISTRIBUTOR, InventoryEventChannel.Distributor)]
    [InlineData(Channel.WHOLESALE, InventoryEventChannel.Wholesale)]
    [InlineData(Channel.OTHER_STORES, InventoryEventChannel.OtherStores)]
    [InlineData(Channel.PRODUCTION, InventoryEventChannel.Production)]
    [InlineData(Channel.EMPLOYEE, InventoryEventChannel.Employee)]
    [InlineData(Channel.MARKETING, InventoryEventChannel.Marketing)]
    [InlineData(Channel.PROCUREMENT, InventoryEventChannel.Procurement)]
    [InlineData(Channel.FINANCE, InventoryEventChannel.Finance)]
    [InlineData(Channel.INTERCOMPANY_DISTRIBUTION, InventoryEventChannel.IntercompanyDistribution)]
    [InlineData(Channel.WHOLESALE_EDI, InventoryEventChannel.WholesaleEdi)]
    public void ToChannel_EveryEnumValue_MapsBySymbolName(Channel source, InventoryEventChannel expected)
    {
        Assert.Equal(expected, InventoryStateChangedEventMapper.ToChannel(source));
    }

    [Theory(DisplayName = "ToChangeType maps every InventoryChangeType enum value by symbol name, including Unknown for the enum's own UNKNOWN member")]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.UNKNOWN, InventoryEventChangeType.Unknown)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.BLC, InventoryEventChangeType.Blc)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.CIE, InventoryEventChangeType.Cie)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.CIN, InventoryEventChangeType.Cin)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.CMD, InventoryEventChangeType.Cmd)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.ENT, InventoryEventChangeType.Ent)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.MAV, InventoryEventChangeType.Mav)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.MIN, InventoryEventChangeType.Min)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.MRP, InventoryEventChangeType.Mrp)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.MPR, InventoryEventChangeType.Mpr)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.MQA, InventoryEventChangeType.Mqa)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.MQP, InventoryEventChangeType.Mqp)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.OIA, InventoryEventChangeType.Oia)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.RFR, InventoryEventChangeType.Rfr)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.RRT, InventoryEventChangeType.Rrt)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.RTR, InventoryEventChangeType.Rtr)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.PICKEDB2B, InventoryEventChangeType.PickedB2B)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.PICKEDB2C, InventoryEventChangeType.PickedB2C)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryChangeType.DGP, InventoryEventChangeType.Dgp)]
    public void ToChangeType_EveryEnumValue_MapsBySymbolName(net.pandora.nexus.@object.inventory.InventoryChangeType source, InventoryEventChangeType expected)
    {
        Assert.Equal(expected, InventoryStateChangedEventMapper.ToChangeType(source));
    }

    [Theory(DisplayName = "ToLocation maps every LocationType enum value by symbol name, including Unknown for the enum's own UNKNOWN member")]
    [InlineData(LocationType.UNKNOWN, InventoryEventLocationType.Unknown)]
    [InlineData(LocationType.WAREHOUSE, InventoryEventLocationType.Warehouse)]
    [InlineData(LocationType.STORE, InventoryEventLocationType.Store)]
    [InlineData(LocationType.DARK_STORE, InventoryEventLocationType.DarkStore)]
    [InlineData(LocationType.TRANSIT, InventoryEventLocationType.Transit)]
    [InlineData(LocationType.PRODUCTION_SITE, InventoryEventLocationType.ProductionSite)]
    [InlineData(LocationType.ZONE, InventoryEventLocationType.Zone)]
    [InlineData(LocationType.THIRD_PARTY_LOGISTICS, InventoryEventLocationType.ThirdPartyLogistics)]
    [InlineData(LocationType.DIGITAL, InventoryEventLocationType.Digital)]
    [InlineData(LocationType.WAREHOUSE_GROUP, InventoryEventLocationType.WarehouseGroup)]
    public void ToLocation_EveryLocationTypeEnumValue_MapsBySymbolName(LocationType type, InventoryEventLocationType expected)
    {
        var result = InventoryStateChangedEventMapper.ToLocation(new Location { id = "L-1", type = type });

        Assert.Equal("L-1", result.Id);
        Assert.Equal(expected, result.Type);
    }

    [Theory(DisplayName = "ToStateSnapshot maps every State enum value by symbol name, including Unknown for the enum's own UNKNOWN member")]
    [InlineData(net.pandora.nexus.@object.inventory.State.UNKNOWN, InventoryEventStockState.Unknown)]
    [InlineData(net.pandora.nexus.@object.inventory.State.AVAILABLE, InventoryEventStockState.Available)]
    [InlineData(net.pandora.nexus.@object.inventory.State.BLOCKED, InventoryEventStockState.Blocked)]
    [InlineData(net.pandora.nexus.@object.inventory.State.INSPECTION, InventoryEventStockState.Inspection)]
    [InlineData(net.pandora.nexus.@object.inventory.State.SCRAP, InventoryEventStockState.Scrap)]
    [InlineData(net.pandora.nexus.@object.inventory.State.REWORK, InventoryEventStockState.Rework)]
    [InlineData(net.pandora.nexus.@object.inventory.State.REMELT, InventoryEventStockState.Remelt)]
    [InlineData(net.pandora.nexus.@object.inventory.State.STONE, InventoryEventStockState.Stone)]
    [InlineData(net.pandora.nexus.@object.inventory.State.AVAILABLETOSELL, InventoryEventStockState.AvailableToSell)]
    public void ToStateSnapshot_EveryStateEnumValue_MapsBySymbolName(net.pandora.nexus.@object.inventory.State state, InventoryEventStockState expected)
    {
        var result = InventoryStateChangedEventMapper.ToStateSnapshot(new net.pandora.nexus.@object.inventory.InventoryState
        {
            state = state,
            status = net.pandora.nexus.@object.inventory.Status.PICKABLE,
        });

        Assert.Equal(expected, result.State);
    }

    [Theory(DisplayName = "ToStateSnapshot maps every Status enum value by symbol name, including Unknown for the enum's own UNKNOWN member")]
    [InlineData(net.pandora.nexus.@object.inventory.Status.UNKNOWN, InventoryEventStockStatus.Unknown)]
    [InlineData(net.pandora.nexus.@object.inventory.Status.PICKABLE, InventoryEventStockStatus.Pickable)]
    [InlineData(net.pandora.nexus.@object.inventory.Status.HELD, InventoryEventStockStatus.Held)]
    [InlineData(net.pandora.nexus.@object.inventory.Status.PREPARED, InventoryEventStockStatus.Prepared)]
    [InlineData(net.pandora.nexus.@object.inventory.Status.HALLMARKING, InventoryEventStockStatus.Hallmarking)]
    [InlineData(net.pandora.nexus.@object.inventory.Status.ALLOCATED, InventoryEventStockStatus.Allocated)]
    [InlineData(net.pandora.nexus.@object.inventory.Status.INVOICED, InventoryEventStockStatus.Invoiced)]
    public void ToStateSnapshot_EveryStatusEnumValue_MapsBySymbolName(net.pandora.nexus.@object.inventory.Status status, InventoryEventStockStatus expected)
    {
        var result = InventoryStateChangedEventMapper.ToStateSnapshot(new net.pandora.nexus.@object.inventory.InventoryState
        {
            state = net.pandora.nexus.@object.inventory.State.AVAILABLE,
            status = status,
        });

        Assert.Equal(expected, result.Status);
    }

    [Theory(DisplayName = "ToItemLine maps every WeightUnit enum value by symbol name, including Unknown for the enum's own UNKNOWN member")]
    [InlineData(WeightUnit.UNKNOWN, InventoryEventWeightUnit.Unknown)]
    [InlineData(WeightUnit.MILLIGRAM, InventoryEventWeightUnit.Milligram)]
    [InlineData(WeightUnit.GRAM, InventoryEventWeightUnit.Gram)]
    [InlineData(WeightUnit.KILOGRAM, InventoryEventWeightUnit.Kilogram)]
    [InlineData(WeightUnit.CARAT, InventoryEventWeightUnit.Carat)]
    public void ToItemLine_EveryWeightUnitEnumValue_MapsBySymbolName(WeightUnit unit, InventoryEventWeightUnit expected)
    {
        var result = InventoryStateChangedEventMapper.ToItemLine(CreateItemLine(netWeight: new Weight { unit = unit, quantity = 1 }));

        Assert.NotNull(result.NetWeight);
        Assert.Equal(expected, result.NetWeight!.Unit);
    }

    [Fact(DisplayName = "ToItemLine maps a null unitPrice to a null UnitPrice, not a default Money")]
    public void ToItemLine_NullUnitPrice_ReturnsNullUnitPrice()
    {
        var result = InventoryStateChangedEventMapper.ToItemLine(CreateItemLine(unitPrice: null));

        Assert.Null(result.UnitPrice);
    }

    private static net.pandora.nexus.@object.inventory.ItemLine CreateItemLine(Weight? netWeight = null, Money? unitPrice = null) => new()
    {
        lineNum = "1",
        productId = "SKU-X",
        itemName = "Item",
        quantity = 1,
        units = "EA",
        countryOfOrigin = CountryCode.TH,
        hallmarking = "NON",
        netWeight = netWeight,
        tareWeight = null,
        unitPrice = unitPrice,
        commodityCode = "7113",
        itemCategoryLocalized = null,
        itemMaterialNameLocalized = null,
        inventoryRegistrationId = null,
        customsRegistrationLineNum = null,
        isBonded = false,
    };
}
