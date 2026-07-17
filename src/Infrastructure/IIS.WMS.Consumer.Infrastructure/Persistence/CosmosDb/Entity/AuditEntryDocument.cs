using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

/// <summary>
/// Cosmos DB persistence shape for an audit record (cosmos-db.instructions.md §3). Kept separate from
/// <c>Domain.Aggregates.AuditEntry</c> so the Domain layer never references Newtonsoft.Json - only
/// <c>AuditEntryMapper</c> and <c>AuditRepository</c> see this type.
/// </summary>
public sealed class AuditEntryDocument : ICosmosDocument
{
    /// <summary>Deterministic-enough item id - <c>{EntityId}:{NewGuid}</c>.</summary>
    public string Id { get; init; } = default!;

    /// <summary><c>{ContainerName}_{EntityPartitionKey}</c> - also this entity's Cosmos partition key.</summary>
    public string Category { get; init; } = default!;

    /// <summary>Cosmos partition key value - identical to <see cref="Category"/> for this entity.</summary>
    public string PartitionKey { get; init; } = default!;

    /// <summary>Name of the Cosmos container the mutated entity lives in.</summary>
    public string ContainerName { get; init; } = default!;

    /// <summary>Id of the entity that was mutated, in its own container.</summary>
    public string EntityId { get; init; } = default!;

    /// <summary>Partition key of the entity that was mutated, in its own container.</summary>
    public string EntityPartitionKey { get; init; } = default!;

    /// <summary>The kind of mutation, stored by name (e.g. "Create") for readability in Data Explorer/Kusto-style queries.</summary>
    public string Operation { get; init; } = default!;

    /// <summary>Correlation id from <c>ICorrelationContext</c> at the time of the mutation.</summary>
    public string CorrelationId { get; init; } = default!;

    /// <summary>First value of <c>ICorrelationContext.Types</c> at the time of the mutation, or empty.</summary>
    public string Schema { get; init; } = default!;

    /// <summary>
    /// The mutated entity's full new-state document, as a nested JSON object rather than an escaped
    /// string - <see langword="null"/> for a delete, since there is no new state.
    /// </summary>
    public JObject? Document { get; init; }

    /// <summary>UTC timestamp the mutation was captured.</summary>
    public DateTime TimestampUtc { get; init; }

    /// <summary>Cosmos's system-managed optimistic-concurrency token. Mapped from <c>_etag</c> since the camelCase naming policy elsewhere can't produce that name.</summary>
    [JsonProperty("_etag")]
    public string? ETag { get; init; }
}
