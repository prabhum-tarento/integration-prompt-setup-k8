using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.ValueObjects;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>
/// Maps between the Domain aggregate and its Cosmos persistence document - the only place either
/// type's shape needs to be known together. <see cref="OrderArchive.OrderDetail"/>'s Value Object
/// carries its own JSON text (<see cref="OrderDetail.Json"/>) and never references Newtonsoft.Json (see
/// its own remarks); this mapper is the only place that text is parsed into/rendered back out of the
/// <see cref="OrderArchiveDocument.OrderDetail"/> nested-JSON shape Cosmos actually stores.
/// </summary>
internal static class OrderArchiveMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static OrderArchiveDocument ToDocument(OrderArchive aggregate) => new()
    {
        Id = aggregate.Id,
        Category = aggregate.Category,
        OrderDetail = JObject.Parse(aggregate.OrderDetail.Json),
        CorrelationId = aggregate.CorrelationId,
        Timestamp = aggregate.Timestamp,
        ETag = aggregate.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos, carrying its ETag along.</summary>
    public static OrderArchive ToDomain(OrderArchiveDocument document)
    {
        // document.OrderDetail can be null here for a selective-column read that didn't select it
        // (cosmos-db.instructions.md §8/§5) - parsing is skipped (Rehydrate tolerates a null OrderDetail)
        // rather than NullReferenceException-ing, matching the plain-assignment tolerance every other
        // property on this mapper already has.
        var orderDetail = document.OrderDetail is null
            ? null
            : OrderDetail.FromJson(document.OrderDetail.ToString(Formatting.None));

        var aggregate = OrderArchive.Rehydrate(document.Id, document.Category, orderDetail, document.CorrelationId, document.Timestamp);
        aggregate.ETag = document.ETag;

        return aggregate;
    }
}
