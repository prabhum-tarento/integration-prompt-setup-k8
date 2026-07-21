using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// Archival copy of one raw inbound message (Kafka or Service Bus), persisted independently to Cosmos
/// DB and/or Blob Storage per <c>MessageArchiveOptions.CosmosDbEnabled</c>/<c>BlobEnabled</c> - unlike
/// <see cref="OrderArchive"/>, this aggregate has no invariants beyond its required fields and raises no
/// domain events; it is a write-once diagnostic record, not a consistency boundary the rest of the
/// domain reasons about.
/// </summary>
public sealed class MessageArchive : AggregateRoot
{
    /// <summary>Caller-supplied category - also this entity's Cosmos partition key.</summary>
    public string Category { get; private init; } = default!;

    /// <summary>The archived message's raw JSON body, stored verbatim - never parsed or validated by this aggregate.</summary>
    public string Payload { get; private init; } = default!;

    /// <summary>Correlation id of the message this record was archived from - also embedded in <see cref="AggregateRoot.Id"/>.</summary>
    public string CorrelationId { get; private init; } = default!;

    /// <summary>UTC timestamp this record was archived.</summary>
    public DateTime Timestamp { get; private init; }

    /// <summary>
    /// Opaque optimistic-concurrency token populated by the repository. Not guarded on - the
    /// Infrastructure repository upserts unconditionally, since a redelivered message archiving twice
    /// under the same deterministic <see cref="AggregateRoot.Id"/> is expected, not a concurrency
    /// conflict to detect.
    /// </summary>
    public string? ETag { get; set; }

    /// <summary>Parameterless so the object initializers in <see cref="Create"/> and <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private MessageArchive()
    {
    }

    /// <summary>
    /// Creates a new archive record. <paramref name="id"/> is the caller's own deterministic identifier,
    /// so a redelivered message naturally upserts rather than duplicating. <paramref name="payload"/> is
    /// stored as-is - a plain raw JSON string, never parsed into a Value Object.
    /// </summary>
    public static MessageArchive Create(string id, string category, string payload, string correlationId, DateTime timestamp)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(category);

        return new MessageArchive
        {
            Id = id,
            Category = category,
            Payload = payload ?? string.Empty,
            CorrelationId = correlationId ?? string.Empty,
            Timestamp = timestamp,
        };
    }

    /// <summary>
    /// Rehydrates an aggregate from persisted state - the repository mapper's entry point, not for new
    /// items. Assigns without validation, tolerating <see langword="null"/> - required for
    /// cosmos-db.instructions.md §8's selective-column reads, which can hand this a document with
    /// <c>Payload</c> unset.
    /// </summary>
    public static MessageArchive Rehydrate(string id, string category, string? payload, string correlationId, DateTime timestamp) => new()
    {
        Id = id,
        Category = category,
        Payload = payload!,
        CorrelationId = correlationId,
        Timestamp = timestamp,
    };
}
