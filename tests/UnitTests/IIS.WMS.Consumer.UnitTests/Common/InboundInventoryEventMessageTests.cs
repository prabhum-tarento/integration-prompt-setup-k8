using IIS.WMS.Common.Messaging;

namespace IIS.WMS.Consumer.UnitTests.Common;

/// <summary>
/// Correctness tests for <see cref="InboundInventoryEventMessage"/> - the wire contract carried end
/// to end from the Kafka topic through the Service Bus relay
/// (integration-resiliency.instructions.md §1).
/// </summary>
public class InboundInventoryEventMessageTests
{
    [Fact(DisplayName = "The constructor exposes every positional parameter on the matching property")]
    public void Constructor_GivenValues_ExposesThemOnProperties()
    {
        var message = new InboundInventoryEventMessage("evt-1", "WH1", "SKU1", 10, "Create");

        Assert.Equal("evt-1", message.EventId);
        Assert.Equal("WH1", message.WarehouseId);
        Assert.Equal("SKU1", message.Sku);
        Assert.Equal(10, message.Quantity);
        Assert.Equal("Create", message.EventType);
    }

    [Fact(DisplayName = "Two instances with the same values are equal, per record value semantics")]
    public void Equals_SameValues_ReturnsTrue()
    {
        var first = new InboundInventoryEventMessage("evt-1", "WH1", "SKU1", 10, "Create");
        var second = new InboundInventoryEventMessage("evt-1", "WH1", "SKU1", 10, "Create");

        Assert.Equal(first, second);
        Assert.True(first == second);
    }

    [Fact(DisplayName = "Two instances differing by one field are not equal")]
    public void Equals_DifferentQuantity_ReturnsFalse()
    {
        var first = new InboundInventoryEventMessage("evt-1", "WH1", "SKU1", 10, "Create");
        var second = new InboundInventoryEventMessage("evt-1", "WH1", "SKU1", 20, "Create");

        Assert.NotEqual(first, second);
        Assert.True(first != second);
    }

    [Fact(DisplayName = "A with-expression produces a copy with only the targeted field changed")]
    public void With_ChangesEventType_LeavesOtherFieldsUnchanged()
    {
        var original = new InboundInventoryEventMessage("evt-1", "WH1", "SKU1", 10, "Create");

        var updated = original with { EventType = "Reserve" };

        Assert.Equal("Reserve", updated.EventType);
        Assert.Equal(original.EventId, updated.EventId);
        Assert.Equal(original.WarehouseId, updated.WarehouseId);
        Assert.Equal(original.Sku, updated.Sku);
        Assert.Equal(original.Quantity, updated.Quantity);
    }
}
