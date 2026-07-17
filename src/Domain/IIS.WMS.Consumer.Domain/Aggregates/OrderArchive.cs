using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Domain.ValueObjects;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// Archival copy of one relayed Kafka event, written to Cosmos DB before the event is published to
/// Service Bus - see <c>ConsumerHostedService.ProcessMessageAsync</c>. Only schemas that opt in via
/// a <c>getOrderArchiveKey</c> selector (<c>ConsumerHostedService.CreateSchemaHandler</c>) produce
/// one of these; unlike <see cref="InventoryEvent"/>, this aggregate has no invariants beyond its
/// required fields and raises no domain events - it is a write-once audit record, not a consistency
/// boundary the rest of the domain reasons about.
/// </summary>
public sealed class OrderArchive : AggregateRoot
{
    /// <summary>Caller-supplied category - also this entity's Cosmos partition key.</summary>
    public string Category { get; private init; } = default!;

    /// <summary>The relayed event's JSON body, as archived at the time of processing.</summary>
    public OrderDetail OrderDetail { get; private init; } = default!;

    /// <summary>Correlation id of the Kafka message this record was archived from - also embedded in <see cref="AggregateRoot.Id"/>.</summary>
    public string CorrelationId { get; private init; } = default!;

    /// <summary>UTC timestamp this record was archived.</summary>
    public DateTime Timestamp { get; private init; }

    /// <summary>
    /// Opaque optimistic-concurrency token populated by the repository. Not guarded on - the
    /// Infrastructure repository upserts unconditionally, since a redelivered event archiving twice
    /// under the same deterministic <see cref="AggregateRoot.Id"/> is expected, not a concurrency
    /// conflict to detect.
    /// </summary>
    public string? ETag { get; set; }

    /// <summary>Parameterless so the object initializers in <see cref="Create"/> and <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private OrderArchive()
    {
    }

    /// <summary>
    /// Creates a new archive record. <paramref name="id"/> is the caller's deterministic
    /// <c>{SchemaName}_{CorrelationId}</c> composite, so a redelivered event naturally upserts rather
    /// than duplicating. <paramref name="orderDetailJson"/> is the caller's raw JSON body - parsed into
    /// the <see cref="ValueObjects.OrderDetail"/> Value Object here, so a caller never constructs that
    /// type directly. <paramref name="correlationId"/> is stored as its own field (not just embedded in
    /// <paramref name="id"/>) so it can be logged/queried without parsing the composite id.
    /// </summary>
    public static OrderArchive Create(string id, string category, string orderDetailJson, string correlationId, DateTime timestamp)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(category);

        return new OrderArchive
        {
            Id = id,
            Category = category,
            OrderDetail = OrderDetail.FromJson(orderDetailJson),
            CorrelationId = correlationId ?? string.Empty,
            Timestamp = timestamp,
        };
    }

    /// <summary>
    /// Rehydrates an aggregate from persisted state - the repository mapper's entry point, not for new
    /// items. Takes the already-parsed <see cref="ValueObjects.OrderDetail"/> directly (rather than raw
    /// JSON) and assigns it without validation, tolerating <see langword="null"/> - required for
    /// cosmos-db.instructions.md §8's selective-column reads, which can hand this a document with
    /// <c>OrderDetail</c> unset.
    /// </summary>
    public static OrderArchive Rehydrate(string id, string category, OrderDetail? orderDetail, string correlationId, DateTime timestamp) => new()
    {
        Id = id,
        Category = category,
        OrderDetail = orderDetail!,
        CorrelationId = correlationId,
        Timestamp = timestamp,
    };
}
