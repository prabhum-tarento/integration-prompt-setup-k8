using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>
/// Cosmos DB persistence shape for an order-archive record (cosmos-db.instructions.md §3). Kept
/// separate from <c>Domain.Aggregates.OrderArchive</c> so the Domain layer never references
/// Newtonsoft.Json - only <c>OrderArchiveMapper</c> and the repository see this type.
/// </summary>
public sealed class OrderArchiveDocument : ICosmosDocument
{
    /// <summary>Deterministic item id - <c>{SchemaName}_{CorrelationId}</c>.</summary>
    public string Id { get; init; } = default!;

    /// <summary>Caller-supplied category this record was archived under.</summary>
    public string Category { get; init; } = default!;

    /// <summary>Cosmos partition key value - identical to <see cref="Category"/> for this entity.</summary>
    public string PartitionKey { get; init; } = default!;

    /// <summary>
    /// The relayed event's JSON body (the mapped <c>EventMapper</c> result object), as archived at the
    /// time of processing - stored as a nested JSON object rather than an escaped string, so it reads
    /// and queries as structured JSON in Cosmos rather than an opaque blob.
    /// </summary>
    public JObject OrderDetail { get; init; } = default!;

    /// <summary>Correlation id of the Kafka message this record was archived from.</summary>
    public string CorrelationId { get; init; } = default!;

    /// <summary>UTC timestamp this record was archived.</summary>
    public DateTime Timestamp { get; init; }

    /// <summary>Cosmos's system-managed optimistic-concurrency token. Mapped from <c>_etag</c> since the camelCase naming policy elsewhere can't produce that name.</summary>
    [JsonProperty("_etag")]
    public string? ETag { get; init; }
}
