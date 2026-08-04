using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;

/// <summary>Maps between the Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class ItemDiscrepencyDetailMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static ItemDiscrepencyDetailDocument ToDocument(ItemDiscrepencyDetail aggregate) => new()
    {
        Id = aggregate.Id,
        Category = aggregate.Category,
        ItemCode = aggregate.ItemCode,
        CountryOfOrigin = aggregate.CountryOfOrigin,
        Hallmark = aggregate.Hallmark,
        IISAvlQty = aggregate.IISAvlQty,
        ReflexAvlQty = aggregate.ReflexAvlQty,
        MasterDataExists = aggregate.MasterDataExists,
        FulfilmentCode = aggregate.FulfilmentCode,
        ETag = aggregate.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos, carrying its ETag along.</summary>
    public static ItemDiscrepencyDetail ToDomain(ItemDiscrepencyDetailDocument document)
    {
        var aggregate = ItemDiscrepencyDetail.Rehydrate(
            document.Id, document.Category, document.ItemCode, document.CountryOfOrigin, document.Hallmark,
            document.IISAvlQty, document.ReflexAvlQty, document.MasterDataExists, document.FulfilmentCode);
        aggregate.ETag = document.ETag;

        return aggregate;
    }
}
