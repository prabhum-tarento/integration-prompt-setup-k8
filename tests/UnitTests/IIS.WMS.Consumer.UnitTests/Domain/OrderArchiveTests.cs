using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.ValueObjects;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>
/// Business-rule tests for the <see cref="OrderArchive"/> aggregate - a write-once audit record with
/// no invariants beyond its required fields and no domain events (see the aggregate's own doc comment),
/// mirroring <c>AuditEntryTests</c>'s style for the sibling write-once aggregate.
/// </summary>
public class OrderArchiveTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "Create parses the raw JSON into OrderDetail and assigns every property")]
    public void Create_ValidInputs_ParsesOrderDetailJsonAndAssignsProperties()
    {
        var archive = OrderArchive.Create(
            id: "InventoryStateChanged_corr-1",
            category: "InventoryStateChanged",
            orderDetailJson: "{\"sku\":\"SKU1\",\"quantity\":10}",
            correlationId: "corr-1",
            timestamp: Now);

        Assert.Equal("InventoryStateChanged_corr-1", archive.Id);
        Assert.Equal("InventoryStateChanged", archive.Category);
        Assert.Equal("{\"sku\":\"SKU1\",\"quantity\":10}", archive.OrderDetail.Json);
        Assert.Equal("corr-1", archive.CorrelationId);
        Assert.Equal(Now, archive.Timestamp);
        Assert.Null(archive.ETag);
    }

    [Theory(DisplayName = "Create throws when a required field is blank")]
    [InlineData("", "InventoryStateChanged")]
    [InlineData("id-1", "")]
    public void Create_BlankRequiredField_Throws(string id, string category)
    {
        Assert.Throws<ArgumentException>(() => OrderArchive.Create(id, category, "{}", "corr-1", Now));
    }

    [Fact(DisplayName = "Create propagates the JsonException thrown by OrderDetail.FromJson for malformed JSON")]
    public void Create_MalformedOrderDetailJson_ThrowsJsonException()
    {
        Assert.Throws<System.Text.Json.JsonException>(() => OrderArchive.Create("id-1", "Category", "not-json", "corr-1", Now));
    }

    [Fact(DisplayName = "Rehydrate assigns every property verbatim without re-parsing")]
    public void Rehydrate_PersistedState_AssignsPropertiesVerbatim()
    {
        var orderDetail = OrderDetail.FromJson("{\"sku\":\"SKU1\"}");

        var archive = OrderArchive.Rehydrate("id-1", "InventoryStateChanged", orderDetail, "corr-1", Now);

        Assert.Equal("id-1", archive.Id);
        Assert.Equal("InventoryStateChanged", archive.Category);
        Assert.Same(orderDetail, archive.OrderDetail);
        Assert.Equal("corr-1", archive.CorrelationId);
        Assert.Equal(Now, archive.Timestamp);
    }

    [Fact(DisplayName = "Rehydrate tolerates a null OrderDetail - the shape a selective-column Cosmos read can produce")]
    public void Rehydrate_NullOrderDetail_IsAllowed()
    {
        var archive = OrderArchive.Rehydrate("id-1", "InventoryStateChanged", orderDetail: null, "corr-1", Now);

        Assert.Null(archive.OrderDetail);
    }

    [Fact(DisplayName = "ETag is settable after construction for the repository to populate post-persist")]
    public void ETag_SetAfterCreate_IsReadable()
    {
        var archive = OrderArchive.Create("id-1", "Category", "{}", "corr-1", Now);

        archive.ETag = "etag-value";

        Assert.Equal("etag-value", archive.ETag);
    }
}
