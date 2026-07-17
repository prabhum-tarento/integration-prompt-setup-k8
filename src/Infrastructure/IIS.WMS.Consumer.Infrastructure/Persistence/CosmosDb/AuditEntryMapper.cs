using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>
/// Maps between the <see cref="AuditEntry"/> aggregate and its Cosmos persistence document - the only
/// place either type's shape needs to be known together. Mirrors <c>OrderArchiveMapper</c>'s
/// nested-JSON convention for the document body.
/// </summary>
internal static class AuditEntryMapper
{
    /// <summary>Projects an aggregate's current state into the persistence shape for a write.</summary>
    public static AuditEntryDocument ToDocument(AuditEntry aggregate) => new()
    {
        Id = aggregate.Id,
        Category = aggregate.Category,
        PartitionKey = aggregate.Category,
        ContainerName = aggregate.ContainerName,
        EntityId = aggregate.EntityId,
        EntityPartitionKey = aggregate.EntityPartitionKey,
        Operation = aggregate.Operation.ToString(),
        CorrelationId = aggregate.CorrelationId,
        Schema = aggregate.Schema,
        Document = aggregate.DocumentJson is null ? null : JObject.Parse(aggregate.DocumentJson),
        TimestampUtc = aggregate.TimestampUtc,
        ETag = aggregate.ETag,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos, carrying its ETag along.</summary>
    public static AuditEntry ToDomain(AuditEntryDocument document)
    {
        var operation = Enum.TryParse<AuditOperation>(document.Operation, out var parsed) ? parsed : default;

        // document.Document can be null here for a selective-column read that didn't select it, or for
        // a genuine Delete record - parsing is skipped rather than NullReferenceException-ing, matching
        // the plain-assignment tolerance every other mapper in this repo has (cosmos-db.instructions.md §5).
        var documentJson = document.Document?.ToString(Formatting.None);

        var aggregate = AuditEntry.Rehydrate(
            document.Id,
            document.Category,
            document.ContainerName,
            document.EntityId,
            document.EntityPartitionKey,
            operation,
            document.CorrelationId,
            document.Schema,
            documentJson,
            document.TimestampUtc);

        aggregate.ETag = document.ETag;

        return aggregate;
    }
}
