using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>Maps between the Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class InventoryBulkImportItemMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static InventoryBulkImportItemDocument ToDocument(InventoryBulkImportItem aggregate) => new()
    {
        Id = aggregate.Id,
        WarehouseId = aggregate.WarehouseId,
        Sku = aggregate.Sku,
        PartitionKey = aggregate.PartitionKey,
        Quantity = aggregate.Quantity,
        SourceSystem = aggregate.SourceSystem,
        SourceLastUpdatedUtc = aggregate.SourceLastUpdatedUtc,
        ETag = aggregate.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos, carrying its ETag along.</summary>
    public static InventoryBulkImportItem ToDomain(InventoryBulkImportItemDocument document)
    {
        var aggregate = InventoryBulkImportItem.Rehydrate(
            document.Id, document.WarehouseId, document.Sku, document.Quantity,
            document.SourceSystem, document.SourceLastUpdatedUtc);

        aggregate.ETag = document.ETag;

        return aggregate;
    }
}
