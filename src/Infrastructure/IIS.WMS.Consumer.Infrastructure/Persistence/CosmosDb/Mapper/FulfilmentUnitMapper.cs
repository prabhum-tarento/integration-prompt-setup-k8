using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;

/// <summary>Maps between the Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class FulfilmentUnitMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static FulfilmentUnitDocument ToDocument(FulfilmentUnit aggregate) => new()
    {
        Id = aggregate.FulfilmentId,
        Category = $"FU_{aggregate.FulfilmentId}",
        CountryCode = aggregate.CountryCode,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos.</summary>
    public static FulfilmentUnit ToDomain(FulfilmentUnitDocument document) => new()
    {
        FulfilmentId = document.Id,
        CountryCode = document.CountryCode,
    };
}
