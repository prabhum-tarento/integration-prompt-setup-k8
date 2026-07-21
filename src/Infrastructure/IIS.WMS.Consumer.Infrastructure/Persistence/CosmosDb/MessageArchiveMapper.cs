using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>
/// Maps between the Domain aggregate and its Cosmos persistence document - the only place either
/// type's shape needs to be known together. Unlike <c>OrderArchiveMapper</c>, <see cref="MessageArchive.Payload"/>
/// is already a plain string on both sides, so no JSON parse/serialize step is needed here.
/// </summary>
internal static class MessageArchiveMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static MessageArchiveDocument ToDocument(MessageArchive aggregate) => new()
    {
        Id = aggregate.Id,
        Category = aggregate.Category,
        Payload = aggregate.Payload,
        CorrelationId = aggregate.CorrelationId,
        Timestamp = aggregate.Timestamp,
        ETag = aggregate.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos, carrying its ETag along.</summary>
    public static MessageArchive ToDomain(MessageArchiveDocument document)
    {
        var aggregate = MessageArchive.Rehydrate(document.Id, document.Category, document.Payload, document.CorrelationId, document.Timestamp);
        aggregate.ETag = document.ETag;

        return aggregate;
    }
}
