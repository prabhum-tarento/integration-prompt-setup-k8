using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// Records one Cosmos DB mutation (add/edit/delete) made through <c>CosmosRepository{TDomain,TDocument}</c>,
/// for every repository built on it - not specific to one entity. Like <see cref="OrderArchive"/>, this is
/// a write-once audit record, not a consistency boundary the rest of the domain reasons about: it raises no
/// domain events and has no invariants beyond its required fields.
/// </summary>
public sealed class AuditEntry : AggregateRoot
{
    /// <summary>
    /// <c>{ContainerName}_{EntityPartitionKey}</c> - also this record's own Cosmos partition key.
    /// Reuses the mutated entity's own partition key value (not the operation kind) so the Audit
    /// container's write distribution inherits whatever cardinality that entity's partition key
    /// already has (cosmos-db.instructions.md §4) - a bare operation-kind key would concentrate every
    /// write for one container/operation pair into a handful of hot partitions.
    /// </summary>
    public string Category { get; private init; } = default!;

    /// <summary>Name of the Cosmos container the mutated entity lives in.</summary>
    public string ContainerName { get; private init; } = default!;

    /// <summary>Id of the entity that was mutated, in its own container.</summary>
    public string EntityId { get; private init; } = default!;

    /// <summary>Partition key of the entity that was mutated, in its own container.</summary>
    public string EntityPartitionKey { get; private init; } = default!;

    /// <summary>The kind of mutation this record captures.</summary>
    public AuditOperation Operation { get; private init; }

    /// <summary>Correlation id from <c>ICorrelationContext</c> at the time of the mutation.</summary>
    public string CorrelationId { get; private init; } = default!;

    /// <summary>First value of <c>ICorrelationContext.Types</c> at the time of the mutation - empty if none was set.</summary>
    public string Schema { get; private init; } = default!;

    /// <summary>
    /// The mutated entity's full new-state document, as JSON - <see langword="null"/> for
    /// <see cref="AuditOperation.Delete"/>, since there is no new state to capture.
    /// </summary>
    public string? DocumentJson { get; private init; }

    /// <summary>UTC timestamp the mutation was captured.</summary>
    public DateTime TimestampUtc { get; private init; }

    /// <summary>Opaque optimistic-concurrency token populated by the repository once persisted.</summary>
    public string? ETag { get; set; }

    /// <summary>Parameterless so the object initializers in <see cref="Create"/> and <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private AuditEntry()
    {
    }

    /// <summary>
    /// Creates a new audit record for a mutation that already completed successfully against Cosmos.
    /// </summary>
    /// <param name="id">Deterministic-enough id - the caller's convention is <c>{EntityId}:{NewGuid}</c>, so redelivery/retry never collides with a prior attempt's audit record.</param>
    /// <param name="containerName">Name of the container the mutated entity lives in.</param>
    /// <param name="entityId">Id of the mutated entity, in its own container.</param>
    /// <param name="entityPartitionKey">Partition key of the mutated entity, in its own container.</param>
    /// <param name="operation">The kind of mutation.</param>
    /// <param name="correlationId">Correlation id from <c>ICorrelationContext</c>.</param>
    /// <param name="schema">First value of <c>ICorrelationContext.Types</c>, or empty.</param>
    /// <param name="documentJson">The entity's full new-state document as JSON, or <see langword="null"/> for a delete.</param>
    /// <param name="timestampUtc">UTC timestamp the mutation was captured.</param>
    public static AuditEntry Create(
        string id,
        string containerName,
        string entityId,
        string entityPartitionKey,
        AuditOperation operation,
        string correlationId,
        string schema,
        string? documentJson,
        DateTime timestampUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);
        ArgumentException.ThrowIfNullOrWhiteSpace(entityId);
        ArgumentException.ThrowIfNullOrWhiteSpace(entityPartitionKey);

        return new AuditEntry
        {
            Id = id,
            Category = $"{containerName}_{entityPartitionKey}",
            ContainerName = containerName,
            EntityId = entityId,
            EntityPartitionKey = entityPartitionKey,
            Operation = operation,
            CorrelationId = correlationId ?? string.Empty,
            Schema = schema ?? string.Empty,
            DocumentJson = documentJson,
            TimestampUtc = timestampUtc,
        };
    }

    /// <summary>Rehydrates an aggregate from persisted state - the repository mapper's entry point, not for new records.</summary>
    public static AuditEntry Rehydrate(
        string id,
        string category,
        string containerName,
        string entityId,
        string entityPartitionKey,
        AuditOperation operation,
        string correlationId,
        string schema,
        string? documentJson,
        DateTime timestampUtc) => new()
    {
        Id = id,
        Category = category,
        ContainerName = containerName,
        EntityId = entityId,
        EntityPartitionKey = entityPartitionKey,
        Operation = operation,
        CorrelationId = correlationId,
        Schema = schema,
        DocumentJson = documentJson,
        TimestampUtc = timestampUtc,
    };
}
