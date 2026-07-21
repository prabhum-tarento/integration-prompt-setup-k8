using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>Maps between the Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class ItemLevelSegmentationMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static ItemLevelSegmentationDocument ToDocument(ItemLevelSegmentation aggregate) => new()
    {
        Id = $"{aggregate.ItemCode}_{aggregate.CountryOfOriginCode}",
        Category = $"SEG_ITEM_{aggregate.FulfilmentCode}_{aggregate.HallmarkCode}",
        FulfilmentCode = aggregate.FulfilmentCode,
        HallmarkCode = aggregate.HallmarkCode,
        ItemCode = aggregate.ItemCode,
        CountryOfOriginCode = aggregate.CountryOfOriginCode,
        EcomShare = aggregate.EcomShare,
        StoreLeveragePercentage = aggregate.StoreLeveragePercentage,
        ThresholdPercentage = aggregate.ThresholdPercentage,
        IsOMNI = aggregate.IsOMNI,
        CurrentOmniStock = aggregate.CurrentOmniStock,
        CurrentEcomStock = aggregate.CurrentEcomStock,
        StoreShare = aggregate.StoreShare,
        EcomStatus = aggregate.EcomStatus,
        IsExtended = aggregate.IsExtended,
        Notes = aggregate.Notes,
        InTransit = aggregate.InTransit,
        LastModified = aggregate.LastModified,
        IsActive = aggregate.IsActive,
        ETag = aggregate.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos, carrying its ETag along for the next optimistic-concurrency write.</summary>
    public static ItemLevelSegmentation ToDomain(ItemLevelSegmentationDocument document) => new()
    {
        FulfilmentCode = document.FulfilmentCode,
        HallmarkCode = document.HallmarkCode,
        ItemCode = document.ItemCode,
        CountryOfOriginCode = document.CountryOfOriginCode,
        EcomShare = document.EcomShare,
        StoreLeveragePercentage = document.StoreLeveragePercentage,
        ThresholdPercentage = document.ThresholdPercentage,
        IsOMNI = document.IsOMNI,
        CurrentOmniStock = document.CurrentOmniStock,
        CurrentEcomStock = document.CurrentEcomStock,
        StoreShare = document.StoreShare,
        EcomStatus = document.EcomStatus,
        IsExtended = document.IsExtended,
        Notes = document.Notes,
        InTransit = document.InTransit,
        LastModified = document.LastModified,
        IsActive = document.IsActive,
        ETag = document.ETag,
    };
}
