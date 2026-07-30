using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;

/// <summary>Maps between the Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class CountryMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static CountryDocument ToDocument(CountryMaster aggregate) => new()
    {
        Id = aggregate.Code,
        Category = $"Country_{aggregate.Code}",
        Name = aggregate.Name,
        RegionCode = aggregate.RegionCode,
        CountryCode = aggregate.Code,
        IsAX12Market = aggregate.IsAX12Market,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos.</summary>
    // TODO(ai): unresolved precedence conflict — CountryMaster.IsActive has no backing property on
    // CountryDocument, so a round-tripped record always reports IsActive = true regardless of the
    // persisted state until a column is added; no doc in docs/ai/ specifies how this field should be
    // persisted, so this can't be inferred rather than guessed.
    public static CountryMaster ToDomain(CountryDocument document) => new()
    {
        Code = document.CountryCode,
        Name = document.Name,
        RegionCode = document.RegionCode,
        IsAX12Market = document.IsAX12Market,
        IsActive = true,
    };
}
