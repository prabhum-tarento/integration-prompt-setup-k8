using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>Maps between the Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class InventoryEventMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static InventoryEventDocument ToDocument(InventoryEvent aggregate) => new()
    {
        Id = aggregate.Id,
        WarehouseId = aggregate.WarehouseId,
        Sku = aggregate.Sku,
        PartitionKey = aggregate.PartitionKey,
        OnHandQuantity = aggregate.OnHandQuantity,
        ActiveReservations = new Dictionary<string, int>(aggregate.ActiveReservations),
        CreatedUtc = aggregate.CreatedUtc,
        ModifiedUtc = aggregate.ModifiedUtc,
        ETag = aggregate.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos, carrying its ETag along for the next optimistic-concurrency write.</summary>
    public static InventoryEvent ToDomain(InventoryEventDocument document)
    {
        var aggregate = InventoryEvent.Rehydrate(
            document.Id,
            document.WarehouseId,
            document.Sku,
            document.OnHandQuantity,
            document.CreatedUtc,
            document.ModifiedUtc,
            document.ActiveReservations);

        aggregate.ETag = document.ETag;

        return aggregate;
    }
}
