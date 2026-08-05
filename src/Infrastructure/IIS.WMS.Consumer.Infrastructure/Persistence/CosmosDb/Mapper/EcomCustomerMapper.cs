using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;

/// <summary>Maps between the <see cref="EcomCustomer"/> Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class EcomCustomerMapper
{
    /// <summary>
    /// Projects an aggregate's current state into the persistence shape - required by
    /// <c>CosmosRepository&lt;,&gt;</c>'s abstract contract, but never exercised in practice since this
    /// reference data is read-only from this service's perspective.
    /// </summary>
    public static EcomCustomerDocument ToDocument(EcomCustomer aggregate) => new()
    {
        Id = aggregate.Id,
        Category = aggregate.Category,
        FulfilmentId = aggregate.FulfilmentId,
        EcomDcList = [.. aggregate.EcomDcList],
        TdcCustomerId = aggregate.TdcCustomerId,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos.</summary>
    public static EcomCustomer ToDomain(EcomCustomerDocument document) =>
        EcomCustomer.Rehydrate(
            document.Id,
            document.Category,
            document.FulfilmentId,
            document.EcomDcList,
            document.TdcCustomerId);
}
