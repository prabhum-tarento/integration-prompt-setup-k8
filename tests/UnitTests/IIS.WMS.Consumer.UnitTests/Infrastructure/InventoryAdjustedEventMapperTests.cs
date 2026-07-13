using IIS.WMS.Consumer.Infrastructure.Messaging;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using net.pandora.nexus.@event.inventory;
using net.pandora.nexus.shared;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="InventoryAdjustedEventMapper"/> - the hand-written mapping from
/// the Avro-generated <see cref="InventoryAdjusted"/> to this consumer's own decoupled
/// <see cref="InventoryAdjustedEvent"/> wire contract.
/// </summary>
public class InventoryAdjustedEventMapperTests
{
    private static InventoryAdjusted CreateSource() => new()
    {
        channel = Channel.WHOLESALE,
        adjustment = new Adjustment
        {
            referenceId = "ADJ-1",
            adjustmentDate = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            entity = "ORG-1",
            type = net.pandora.nexus.@object.inventory.InventoryChangeType.RFR,
            state = new net.pandora.nexus.@object.inventory.InventoryState
            {
                state = net.pandora.nexus.@object.inventory.State.AVAILABLE,
                status = net.pandora.nexus.@object.inventory.Status.PICKABLE,
            },
            location = new Location { id = "WH-1", type = LocationType.WAREHOUSE },
            reason = net.pandora.nexus.@object.inventory.ReasonCode.RECEIPT,
            adjustmentLines =
            [
                new net.pandora.nexus.@object.inventory.ItemLine
                {
                    lineNum = "1",
                    productId = "SKU-1",
                    itemName = "Bracelet",
                    quantity = 3,
                    units = "EA",
                    countryOfOrigin = CountryCode.TH,
                    hallmarking = "NON",
                    netWeight = new Weight { unit = WeightUnit.GRAM, quantity = 8 },
                    tareWeight = null,
                    unitPrice = new Money { currencyCode = CurrencyCode.USD, units = 29, nanos = 990_000_000 },
                    commodityCode = "7113",
                    itemCategoryLocalized = null,
                    itemMaterialNameLocalized = null,
                    inventoryRegistrationId = null,
                    customsRegistrationLineNum = null,
                    isBonded = false,
                },
            ],
        },
    };

    [Fact(DisplayName = "ToInventoryAdjustedEvent maps channel and adjustment scalar fields as-is")]
    public void ToInventoryAdjustedEvent_ScalarFields_MapsUnchanged()
    {
        var result = CreateSource().ToInventoryAdjustedEvent();

        Assert.Equal(InventoryEventChannel.Wholesale, result.Channel);
        Assert.Equal("ADJ-1", result.Adjustment.ReferenceId);
        Assert.Equal(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc), result.Adjustment.AdjustmentDate);
        Assert.Equal("ORG-1", result.Adjustment.Entity);
    }

    [Fact(DisplayName = "ToInventoryAdjustedEvent maps enums by symbol name, not ordinal")]
    public void ToInventoryAdjustedEvent_Enums_MapsBySymbolName()
    {
        var result = CreateSource().ToInventoryAdjustedEvent();

        Assert.Equal(InventoryEventChangeType.Rfr, result.Adjustment.Type);
        Assert.Equal(InventoryEventStockState.Available, result.Adjustment.State.State);
        Assert.Equal(InventoryEventStockStatus.Pickable, result.Adjustment.State.Status);
        Assert.Equal(InventoryEventLocationType.Warehouse, result.Adjustment.Location.Type);
        Assert.Equal(InventoryEventReasonCode.Receipt, result.Adjustment.Reason);
    }

    [Fact(DisplayName = "ToInventoryAdjustedEvent maps location id unchanged")]
    public void ToInventoryAdjustedEvent_Location_MapsId()
    {
        var result = CreateSource().ToInventoryAdjustedEvent();

        Assert.Equal("WH-1", result.Adjustment.Location.Id);
    }

    [Fact(DisplayName = "ToInventoryAdjustedEvent maps adjustment lines, including nested weight/money and null tareWeight")]
    public void ToInventoryAdjustedEvent_AdjustmentLines_MapsNestedFields()
    {
        var result = CreateSource().ToInventoryAdjustedEvent();

        var line = Assert.Single(result.Adjustment.AdjustmentLines);
        Assert.Equal("1", line.LineNum);
        Assert.Equal("SKU-1", line.ProductId);
        Assert.Equal(3, line.Quantity);
        Assert.Equal("TH", line.CountryOfOrigin);
        Assert.False(line.IsBonded);
        Assert.Null(line.TareWeight);

        Assert.NotNull(line.NetWeight);
        Assert.Equal(InventoryEventWeightUnit.Gram, line.NetWeight!.Unit);
        Assert.Equal(8, line.NetWeight.Quantity);

        Assert.NotNull(line.UnitPrice);
        Assert.Equal("USD", line.UnitPrice!.CurrencyCode);
        Assert.Equal(29.99m, line.UnitPrice.Amount);
    }
}
