using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;

/// <summary>Maps between the <see cref="ItemStockWarehouseInventory"/> Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class ItemStockWarehouseInventoryMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static ItemStockWarehouseInventoryDocument ToDocument(ItemStockWarehouseInventory aggregate) => new()
    {
        Id = aggregate.Id,
        Category = aggregate.Category,
        FulfilmentId = aggregate.FulfilmentId,
        ItemCode = aggregate.ItemCode,
        Qnty = aggregate.Qnty,
        ModifiedUtc = aggregate.ModifiedUtc.ToString("O"),
        ETag = aggregate.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos, carrying its ETag along for the next optimistic-concurrency write.</summary>
    public static ItemStockWarehouseInventory ToDomain(ItemStockWarehouseInventoryDocument document)
    {
        var aggregate = ItemStockWarehouseInventory.Rehydrate(
            document.Id,
            document.FulfilmentId,
            document.ItemCode,
            document.Qnty ?? 0,
            DateTime.TryParse(
                document.ModifiedUtc, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind, out var modifiedUtc)
                ? modifiedUtc
                : DateTime.UtcNow);

        aggregate.ETag = document.ETag;

        return aggregate;
    }
}
