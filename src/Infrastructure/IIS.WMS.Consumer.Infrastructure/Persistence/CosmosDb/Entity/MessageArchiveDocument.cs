using Newtonsoft.Json;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>
/// Cosmos DB persistence shape for a message-archive record (cosmos-db.instructions.md §3). Kept
/// separate from <c>Domain.Aggregates.MessageArchive</c> so the Domain layer never references
/// Newtonsoft.Json - only <c>MessageArchiveMapper</c> and the repository see this type.
/// </summary>
public sealed class MessageArchiveDocument : ICosmosDocument
{
    /// <summary>Deterministic item id.</summary>
    public string Id { get; init; } = default!;

    /// <summary>Caller-supplied category this record was archived under - also this entity's Cosmos partition key.</summary>
    public string Category { get; init; } = default!;

    /// <summary>
    /// The archived message's raw JSON body, stored verbatim as an escaped string - unlike
    /// <c>OrderArchiveDocument.OrderDetail</c>, not parsed into a nested JSON object, since a message
    /// archive's payload shape is not known/validated at this layer.
    /// </summary>
    public string Payload { get; init; } = default!;

    /// <summary>Correlation id of the message this record was archived from.</summary>
    public string CorrelationId { get; init; } = default!;

    /// <summary>UTC timestamp this record was archived.</summary>
    public DateTime Timestamp { get; init; }

    /// <summary>Cosmos's system-managed optimistic-concurrency token. Mapped from <c>_etag</c> since the camelCase naming policy elsewhere can't produce that name.</summary>
    [JsonProperty("_etag")]
    public string? ETag { get; init; }
}
