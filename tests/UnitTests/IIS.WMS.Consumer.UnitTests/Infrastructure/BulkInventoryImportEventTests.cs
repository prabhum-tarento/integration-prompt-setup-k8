using Avro;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.AvroContracts;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="BulkInventoryImportEvent"/> - the hand-written placeholder Avro
/// contract for the bulk-import topic, implementing <see cref="Avro.Specific.ISpecificRecord"/>
/// directly rather than via generated code (see its own remarks).
/// </summary>
public class BulkInventoryImportEventTests
{
    private static BulkInventoryImportEvent CreateSource() => new()
    {
        EventId = "EVT-1",
        WarehouseId = "WH-1",
        Sku = "SKU-1",
        Quantity = 10,
        SourceSystem = "WMS",
        LastUpdatedUtcMillis = 1_700_000_000_000,
    };

    [Fact(DisplayName = "Schema returns the parsed Avro record schema")]
    public void Schema_Always_ReturnsParsedRecordSchema()
    {
        var sut = CreateSource();

        Assert.Same(BulkInventoryImportEvent.AvroSchema, sut.Schema);
        Assert.Equal(Schema.Type.Record, sut.Schema.Tag);
    }

    [Theory(DisplayName = "Get returns the field at each valid field position")]
    [InlineData(0, "EVT-1")]
    [InlineData(1, "WH-1")]
    [InlineData(2, "SKU-1")]
    [InlineData(3, 10)]
    [InlineData(4, "WMS")]
    [InlineData(5, 1_700_000_000_000L)]
    public void Get_ValidFieldPosition_ReturnsFieldValue(int fieldPos, object expected)
    {
        var sut = CreateSource();

        Assert.Equal(expected, sut.Get(fieldPos));
    }

    [Fact(DisplayName = "Get throws AvroRuntimeException for an out-of-range field position")]
    public void Get_InvalidFieldPosition_ThrowsAvroRuntimeException()
    {
        var sut = CreateSource();

        Assert.Throws<AvroRuntimeException>(() => sut.Get(99));
    }

    [Fact(DisplayName = "Put sets EventId at field position 0")]
    public void Put_FieldPosition0_SetsEventId()
    {
        var sut = new BulkInventoryImportEvent();

        sut.Put(0, "EVT-2");

        Assert.Equal("EVT-2", sut.EventId);
    }

    [Fact(DisplayName = "Put sets WarehouseId at field position 1")]
    public void Put_FieldPosition1_SetsWarehouseId()
    {
        var sut = new BulkInventoryImportEvent();

        sut.Put(1, "WH-2");

        Assert.Equal("WH-2", sut.WarehouseId);
    }

    [Fact(DisplayName = "Put sets Sku at field position 2")]
    public void Put_FieldPosition2_SetsSku()
    {
        var sut = new BulkInventoryImportEvent();

        sut.Put(2, "SKU-2");

        Assert.Equal("SKU-2", sut.Sku);
    }

    [Fact(DisplayName = "Put sets Quantity at field position 3")]
    public void Put_FieldPosition3_SetsQuantity()
    {
        var sut = new BulkInventoryImportEvent();

        sut.Put(3, 25);

        Assert.Equal(25, sut.Quantity);
    }

    [Fact(DisplayName = "Put sets SourceSystem at field position 4")]
    public void Put_FieldPosition4_SetsSourceSystem()
    {
        var sut = new BulkInventoryImportEvent();

        sut.Put(4, "ERP");

        Assert.Equal("ERP", sut.SourceSystem);
    }

    [Fact(DisplayName = "Put sets LastUpdatedUtcMillis at field position 5")]
    public void Put_FieldPosition5_SetsLastUpdatedUtcMillis()
    {
        var sut = new BulkInventoryImportEvent();

        sut.Put(5, 1_800_000_000_000L);

        Assert.Equal(1_800_000_000_000L, sut.LastUpdatedUtcMillis);
    }

    [Fact(DisplayName = "Put throws AvroRuntimeException for an out-of-range field position")]
    public void Put_InvalidFieldPosition_ThrowsAvroRuntimeException()
    {
        var sut = new BulkInventoryImportEvent();

        Assert.Throws<AvroRuntimeException>(() => sut.Put(99, "value"));
    }
}
