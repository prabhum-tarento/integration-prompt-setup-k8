using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;

/// <summary>Maps between the Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class ItemStockInventoryExtendedMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static ItemStockInventoryExtendedDocument ToDocument(ItemStockInventoryExtended domain) => new()
    {
        Id = domain.Id,
        Category = domain.Id,
        ItemCode = domain.ItemCode,
        FulfilmentId = domain.FulfilmentId,
        COO = domain.COO,
        Hallmark = domain.Hallmark,
        State = domain.State.ToString(),
        Status = domain.Status.ToString(),
        Qty = domain.Qty,
        Timestamp = domain.SubmittedDate ?? default,
        IsPOSM = domain.IsPOSM,
        ETag = domain.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos.</summary>
    public static ItemStockInventoryExtended ToDomain(ItemStockInventoryExtendedDocument document) => new()
    {
        ItemCode = document.ItemCode,
        FulfilmentId = document.FulfilmentId,
        COO = document.COO,
        Hallmark = document.Hallmark,
        State = Enum.Parse<State>(document.State, ignoreCase: true),
        Status = Enum.Parse<Status>(document.Status, ignoreCase: true),
        Qty = document.Qty,
        SubmittedDate = document.Timestamp,
        IsPOSM = document.IsPOSM,
        ETag = document.ETag,
    };
}
