using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>
/// Business-rule tests for the <see cref="MessageArchive"/> aggregate - a write-once diagnostic record
/// with no invariants beyond its required fields and no domain events (see the aggregate's own doc
/// comment), mirroring <c>OrderArchiveTests</c>'s style for the sibling write-once aggregate. Unlike
/// <c>OrderArchive</c>, <see cref="MessageArchive.Payload"/> is a plain string with no JSON-parsing step,
/// so there is no malformed-payload test here.
/// </summary>
public class MessageArchiveTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "Create assigns every property verbatim")]
    public void Create_ValidInputs_AssignsProperties()
    {
        var archive = MessageArchive.Create(
            id: "InventoryStateChanged_corr-1",
            category: "InventoryStateChanged",
            payload: "{\"sku\":\"SKU1\",\"quantity\":10}",
            correlationId: "corr-1",
            timestamp: Now);

        Assert.Equal("InventoryStateChanged_corr-1", archive.Id);
        Assert.Equal("InventoryStateChanged", archive.Category);
        Assert.Equal("{\"sku\":\"SKU1\",\"quantity\":10}", archive.Payload);
        Assert.Equal("corr-1", archive.CorrelationId);
        Assert.Equal(Now, archive.Timestamp);
        Assert.Null(archive.ETag);
    }

    [Theory(DisplayName = "Create throws when a required field is blank")]
    [InlineData("", "InventoryStateChanged")]
    [InlineData("id-1", "")]
    public void Create_BlankRequiredField_Throws(string id, string category)
    {
        Assert.Throws<ArgumentException>(() => MessageArchive.Create(id, category, "{}", "corr-1", Now));
    }

    [Fact(DisplayName = "Rehydrate assigns every property verbatim without validation")]
    public void Rehydrate_PersistedState_AssignsPropertiesVerbatim()
    {
        var archive = MessageArchive.Rehydrate("id-1", "InventoryStateChanged", "{\"sku\":\"SKU1\"}", "corr-1", Now);

        Assert.Equal("id-1", archive.Id);
        Assert.Equal("InventoryStateChanged", archive.Category);
        Assert.Equal("{\"sku\":\"SKU1\"}", archive.Payload);
        Assert.Equal("corr-1", archive.CorrelationId);
        Assert.Equal(Now, archive.Timestamp);
    }

    [Fact(DisplayName = "ETag is settable after construction for the repository to populate post-persist")]
    public void ETag_SetAfterCreate_IsReadable()
    {
        var archive = MessageArchive.Create("id-1", "Category", "{}", "corr-1", Now);

        archive.ETag = "etag-value";

        Assert.Equal("etag-value", archive.ETag);
    }
}
