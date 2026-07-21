using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>Maps between the <see cref="ItemStockInventory"/> Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class ItemStockInventoryMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static ItemStockInventoryDocument ToDocument(ItemStockInventory aggregate) => new()
    {
        Id = aggregate.Id,
        Category = aggregate.Category,
        ItemCode = aggregate.ItemCode,
        FulfilmentId = aggregate.FulfilmentId,
        COO = aggregate.CountryOfOrigin,
        Hallmark = aggregate.Hallmark,
        B2BAVL = aggregate.B2BAvailable,
        B2CAVL = aggregate.B2CAvailable,
        B2COrg = aggregate.B2COriginal,
        B2CExtended = aggregate.B2CExtended,
        B2CAllocated = aggregate.B2CAllocated,
        B2BAllocated = aggregate.B2BAllocated,
        B2CPrepared = aggregate.B2CPrepared,
        B2BPrepared = aggregate.B2BPrepared,
        InternalHallmarkAllocated = aggregate.InternalHallmarkAllocated,
        InTransit = aggregate.InTransit,
        B2CThreshold = aggregate.B2CThreshold,
        IsExtended = aggregate.IsExtended,
        B2BUsedShare = aggregate.B2BUsedShare,
        Inspection = aggregate.Inspection,
        PSC = aggregate.Psc,
        Timestamp = aggregate.ModifiedUtc.ToString("O"),
        IsPOSM = aggregate.IsPosm,
        ETag = aggregate.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos, carrying its ETag along for the next optimistic-concurrency write.</summary>
    public static ItemStockInventory ToDomain(ItemStockInventoryDocument document)
    {
        var aggregate = ItemStockInventory.Rehydrate(
            document.Id,
            document.FulfilmentId,
            document.ItemCode,
            document.COO,
            document.Hallmark,
            document.B2BAVL ?? 0,
            document.B2CAVL ?? 0,
            document.B2COrg ?? 0,
            document.B2CExtended ?? 0,
            document.B2CAllocated ?? 0,
            document.B2BAllocated ?? 0,
            document.B2CPrepared ?? 0,
            document.B2BPrepared ?? 0,
            document.InternalHallmarkAllocated ?? 0,
            document.InTransit ?? 0,
            document.B2CThreshold ?? 0,
            document.IsExtended,
            document.B2BUsedShare ?? 0,
            document.Inspection ?? 0,
            document.PSC ?? 0,
            document.IsPOSM ?? false,
            DateTime.TryParse(
                document.Timestamp, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind, out var modifiedUtc)
                ? modifiedUtc
                : DateTime.UtcNow);

        aggregate.ETag = document.ETag;

        return aggregate;
    }
}