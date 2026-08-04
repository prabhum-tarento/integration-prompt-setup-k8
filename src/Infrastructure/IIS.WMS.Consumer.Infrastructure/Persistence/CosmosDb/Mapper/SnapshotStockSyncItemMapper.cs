using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;

/// <summary>Maps between the Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class SnapshotStockSyncItemMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static SnapshotStockSyncItemDocument ToDocument(SnapshotStockSyncItem aggregate) => new()
    {
        Id = aggregate.Id,
        Category = aggregate.Category,
        ItemCode = aggregate.ItemCode,
        CountryOfOriginCode = aggregate.CountryOfOriginCode,
        FulfilmentUnit = aggregate.FulfilmentUnit,
        Hallmark = aggregate.Hallmark,
        Quantity = aggregate.Quantity,
        QuantityType = aggregate.QuantityType,
        ETag = aggregate.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos, carrying its ETag along.</summary>
    public static SnapshotStockSyncItem ToDomain(SnapshotStockSyncItemDocument document)
    {
        var aggregate = SnapshotStockSyncItem.Rehydrate(
            document.Id, document.Category, document.ItemCode, document.CountryOfOriginCode,
            document.FulfilmentUnit, document.Hallmark, document.Quantity, document.QuantityType);
        aggregate.ETag = document.ETag;

        return aggregate;
    }
}
