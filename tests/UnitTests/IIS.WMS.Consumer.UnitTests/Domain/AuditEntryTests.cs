using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>Business-rule tests for the <see cref="AuditEntry"/> aggregate - required-field validation and the <see cref="AuditEntry.Category"/> derivation.</summary>
public class AuditEntryTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "Create derives Category from the container name and entity partition key")]
    public void Create_ValidInputs_DerivesCategoryFromContainerAndPartitionKey()
    {
        var entry = AuditEntry.Create(
            id: "WH1:SKU1:11111111-1111-1111-1111-111111111111",
            containerName: "InventoryEvents",
            entityId: "WH1:SKU1",
            entityPartitionKey: "WH1:SKU1",
            operation: AuditOperation.Replace,
            correlationId: "corr-1",
            schema: "InventoryStateChanged",
            documentJson: "{\"sku\":\"SKU1\"}",
            timestampUtc: Now);

        Assert.Equal("InventoryEvents_WH1:SKU1", entry.Category);
        Assert.Equal("InventoryEvents", entry.ContainerName);
        Assert.Equal(AuditOperation.Replace, entry.Operation);
        Assert.Equal("corr-1", entry.CorrelationId);
        Assert.Equal("InventoryStateChanged", entry.Schema);
        Assert.Equal("{\"sku\":\"SKU1\"}", entry.DocumentJson);
    }

    [Fact(DisplayName = "Create allows a null DocumentJson - the shape a Delete operation records")]
    public void Create_NullDocumentJson_IsAllowed()
    {
        var entry = AuditEntry.Create(
            "WH1:SKU1:11111111-1111-1111-1111-111111111111", "InventoryEvents", "WH1:SKU1", "WH1:SKU1",
            AuditOperation.Delete, "corr-1", "InventoryStateChanged", documentJson: null, Now);

        Assert.Null(entry.DocumentJson);
    }

    [Theory(DisplayName = "Create throws when a required field is blank")]
    [InlineData("", "InventoryEvents", "WH1:SKU1", "WH1:SKU1")]
    [InlineData("id-1", "", "WH1:SKU1", "WH1:SKU1")]
    [InlineData("id-1", "InventoryEvents", "", "WH1:SKU1")]
    [InlineData("id-1", "InventoryEvents", "WH1:SKU1", "")]
    public void Create_BlankRequiredField_Throws(string id, string containerName, string entityId, string entityPartitionKey)
    {
        Assert.Throws<ArgumentException>(() => AuditEntry.Create(
            id, containerName, entityId, entityPartitionKey, AuditOperation.Create, "corr-1", "Schema", "{}", Now));
    }

    [Fact(DisplayName = "Rehydrate assigns every property without recomputing Category")]
    public void Rehydrate_PersistedState_AssignsPropertiesVerbatim()
    {
        var entry = AuditEntry.Rehydrate(
            id: "WH1:SKU1:guid",
            category: "InventoryEvents_WH1:SKU1",
            containerName: "InventoryEvents",
            entityId: "WH1:SKU1",
            entityPartitionKey: "WH1:SKU1",
            operation: AuditOperation.Patch,
            correlationId: "corr-2",
            schema: "InventoryAdjusted",
            documentJson: null,
            timestampUtc: Now);

        Assert.Equal("InventoryEvents_WH1:SKU1", entry.Category);
        Assert.Equal(AuditOperation.Patch, entry.Operation);
        Assert.Null(entry.DocumentJson);
    }
}
