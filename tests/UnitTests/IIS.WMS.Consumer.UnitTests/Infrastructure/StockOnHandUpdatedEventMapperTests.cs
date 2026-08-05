using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockOnHandUpdated;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockOnHandUpdated.Mappers;
using net.pandora.nexus.@event.inventory;
using net.pandora.nexus.shared;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="StockOnHandUpdatedEventMapper"/> - the hand-written mapping from
/// the Avro-generated <see cref="StockOnHandUpdated"/> to this consumer's own decoupled
/// <see cref="StockOnHandUpdatedEvent"/> wire contract.
/// </summary>
public class StockOnHandUpdatedEventMapperTests
{
    private static StockOnHandUpdated CreateSource() => new()
    {
        channel = Channel.OWN_ONLINE,
        referenceId = "REF-1",
        updatedDate = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        location = new Location { id = "BRZ3PL-1", type = LocationType.THIRD_PARTY_LOGISTICS },
        entity = "ORG-1",
        market = CountryCode.BR,
        productId = "SKU-1",
        productUnits = "EA",
        barcode = "1234567890",
        quantityDetails =
        [
            new AbsoluteQuantityDetail
            {
                quantity = 5,
                state = new net.pandora.nexus.@object.inventory.InventoryState
                {
                    state = net.pandora.nexus.@object.inventory.State.AVAILABLE,
                    status = net.pandora.nexus.@object.inventory.Status.PICKABLE,
                },
                countryOfOrigin = CountryCode.TH,
                hallmarking = "NON",
                domain = net.pandora.nexus.@object.inventory.InventoryDomain.B2C,
            },
        ],
        reason = net.pandora.nexus.@object.inventory.ReasonCode.RECEIPT,
    };

    [Fact(DisplayName = "ToStockOnHandUpdatedEvent maps scalar fields as-is and does not map the market field")]
    public void ToStockOnHandUpdatedEvent_ScalarFields_MapsUnchanged()
    {
        var result = CreateSource().ToStockOnHandUpdatedEvent();

        Assert.Equal("REF-1", result.ReferenceId);
        Assert.Equal(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc), result.UpdatedDate);
        Assert.Equal("ORG-1", result.Entity);
        Assert.Equal("SKU-1", result.ProductId);
        Assert.Equal("EA", result.ProductUnits);
        Assert.Equal("1234567890", result.Barcode);
    }

    [Fact(DisplayName = "ToStockOnHandUpdatedEvent maps channel and location by delegating to InventoryStateChangedEventMapper")]
    public void ToStockOnHandUpdatedEvent_ChannelAndLocation_MapsBySharedHelpers()
    {
        var result = CreateSource().ToStockOnHandUpdatedEvent();

        Assert.Equal(InventoryEventChannel.OwnOnline, result.Channel);
        Assert.Equal("BRZ3PL-1", result.Location.Id);
        Assert.Equal(InventoryEventLocationType.ThirdPartyLogistics, result.Location.Type);
    }

    [Fact(DisplayName = "ToStockOnHandUpdatedEvent maps quantityDetails including nested state/status and countryOfOrigin as a raw ISO string")]
    public void ToStockOnHandUpdatedEvent_QuantityDetails_MapsNestedFields()
    {
        var result = CreateSource().ToStockOnHandUpdatedEvent();

        var detail = Assert.Single(result.QuantityDetails);
        Assert.Equal(5, detail.Quantity);
        Assert.Equal(InventoryEventStockState.Available, detail.State.State);
        Assert.Equal(InventoryEventStockStatus.Pickable, detail.State.Status);
        Assert.Equal("TH", detail.CountryOfOrigin);
        Assert.Equal("NON", detail.Hallmarking);
        Assert.Equal(StockOnHandInventoryDomain.B2C, detail.Domain);
    }

    [Theory(DisplayName = "ToStockOnHandUpdatedEvent maps every InventoryDomain enum value by symbol name, including Unknown for the enum's own UNKNOWN member")]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryDomain.UNKNOWN, StockOnHandInventoryDomain.Unknown)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryDomain.B2B, StockOnHandInventoryDomain.B2B)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryDomain.B2C, StockOnHandInventoryDomain.B2C)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryDomain.INTERNAL_HALLMARKING, StockOnHandInventoryDomain.InternalHallmarking)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryDomain.EXTERNAL_HALLMARKING, StockOnHandInventoryDomain.ExternalHallmarking)]
    [InlineData(net.pandora.nexus.@object.inventory.InventoryDomain.OMNI, StockOnHandInventoryDomain.Omni)]
    public void ToStockOnHandUpdatedEvent_EveryInventoryDomainEnumValue_MapsBySymbolName(
        net.pandora.nexus.@object.inventory.InventoryDomain domain, StockOnHandInventoryDomain expected)
    {
        var source = CreateSource();
        source.quantityDetails[0].domain = domain;

        var result = source.ToStockOnHandUpdatedEvent();

        Assert.Equal(expected, result.QuantityDetails[0].Domain);
    }

    [Theory(DisplayName = "ToStockOnHandUpdatedEvent maps every ReasonCode enum value by symbol name, including Unknown for the enum's own UNKNOWN member")]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.UNKNOWN, StockOnHandUpdatedReason.Unknown)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.ADJUSTMENT, StockOnHandUpdatedReason.Adjustment)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.BUNDLING, StockOnHandUpdatedReason.Bundling)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.COUNTING, StockOnHandUpdatedReason.Counting)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.CUSTOMER_RETURN, StockOnHandUpdatedReason.CustomerReturn)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.OTHER, StockOnHandUpdatedReason.Other)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.RECEIPT, StockOnHandUpdatedReason.Receipt)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.RECEIPT_ADJUSTMENT, StockOnHandUpdatedReason.ReceiptAdjustment)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.RETURN, StockOnHandUpdatedReason.Return)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.SALE, StockOnHandUpdatedReason.Sale)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.TRANSFER, StockOnHandUpdatedReason.Transfer)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.VENDOR_RETURN, StockOnHandUpdatedReason.VendorReturn)]
    [InlineData(net.pandora.nexus.@object.inventory.ReasonCode.AUTO_RECONCILIATION, StockOnHandUpdatedReason.AutoReconciliation)]
    public void ToStockOnHandUpdatedEvent_EveryReasonCodeEnumValue_MapsBySymbolName(
        net.pandora.nexus.@object.inventory.ReasonCode reason, StockOnHandUpdatedReason expected)
    {
        var source = CreateSource();
        source.reason = reason;

        var result = source.ToStockOnHandUpdatedEvent();

        Assert.Equal(expected, result.Reason);
    }
}
