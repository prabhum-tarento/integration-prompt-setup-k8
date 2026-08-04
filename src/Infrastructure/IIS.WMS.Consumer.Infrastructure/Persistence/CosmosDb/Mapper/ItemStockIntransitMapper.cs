using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;

/// <summary>Maps between the <see cref="ItemStockIntransit"/> Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class ItemStockIntransitMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static ItemStockIntransitDocument ToDocument(ItemStockIntransit aggregate) => new()
    {
        Id = aggregate.Id,
        Category = aggregate.Category,
        ItemCode = aggregate.ItemCode,
        HallmarkCode = aggregate.HallmarkCode,
        CountryOfOriginCode = aggregate.CountryOfOriginCode,
        OrderType = aggregate.OrderType,
        FulfilmentCode = aggregate.FulfilmentCode,
        Status = aggregate.Status,
        Quantity = aggregate.Quantity,
        Timestamp = aggregate.ModifiedUtc.ToString("O"),
        ETag = aggregate.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos, carrying its ETag along for the next optimistic-concurrency write.</summary>
    public static ItemStockIntransit ToDomain(ItemStockIntransitDocument document)
    {
        var aggregate = ItemStockIntransit.Rehydrate(
            document.Id,
            document.ItemCode,
            document.HallmarkCode,
            document.CountryOfOriginCode,
            document.OrderType,
            document.FulfilmentCode,
            document.Status,
            document.Quantity ?? 0,
            DateTime.TryParse(
                document.Timestamp, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind, out var modifiedUtc)
                ? modifiedUtc
                : DateTime.UtcNow);

        aggregate.ETag = document.ETag;

        return aggregate;
    }
}
