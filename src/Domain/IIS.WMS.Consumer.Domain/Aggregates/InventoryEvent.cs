using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Domain.Events;
using IIS.WMS.Consumer.Domain.Exceptions;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// Consistency boundary for one warehouse/SKU's on-hand quantity. Enforces the oversell-prevention
/// invariant (on-hand quantity never goes negative) and models reservation/allocation as an explicit
/// state machine so a reservation and its eventual allocation or release net to zero
/// (dotnet-architecture-good-practices.instructions.md §5). <c>Category</c> is the composite
/// <c>WarehouseId:Sku</c>, matching the Service Bus session id
/// (integration-resiliency.instructions.md §1) and the Cosmos partition key
/// (cosmos-db.instructions.md §4).
/// </summary>
public sealed class InventoryEvent : AggregateRoot
{
    private readonly Dictionary<string, int> reservations = [];

    /// <summary>The warehouse this on-hand balance belongs to - the first half of the composite partition/session key.</summary>
    public string WarehouseId { get; private init; } = default!;

    /// <summary>The SKU this on-hand balance tracks - the second half of the composite partition/session key.</summary>
    public string Sku { get; private init; } = default!;

    /// <summary>The composite <c>WarehouseId:Sku</c> key - matches the Cosmos partition key and the Service Bus session id for this aggregate.</summary>
    public string Category => $"{WarehouseId}:{Sku}";

    /// <summary>Current on-hand quantity, after any active reservations have already been deducted. Never negative.</summary>
    public int OnHandQuantity { get; private set; }

    /// <summary>UTC timestamp the aggregate was first created.</summary>
    public DateTime CreatedUtc { get; private init; }

    /// <summary>UTC timestamp of the most recent state change.</summary>
    public DateTime ModifiedUtc { get; private set; }

    /// <summary>Reservations that have decremented on-hand but not yet been allocated or released, keyed by reservation id.</summary>
    public IReadOnlyDictionary<string, int> ActiveReservations => reservations;

    /// <summary>
    /// Opaque optimistic-concurrency token populated by the repository from the store's native
    /// version marker (Cosmos's <c>_etag</c>). Not a business-meaningful field - the aggregate
    /// carries it only so a caller can read-then-write without a second round trip
    /// (cosmos-db.instructions.md §9).
    /// </summary>
    public string? ETag { get; set; }

    /// <summary>Parameterless so the object initializers in <see cref="Create"/> and <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private InventoryEvent()
    {
    }

    /// <summary>
    /// Creates a new aggregate. <paramref name="id"/> must be deterministic (derived from the
    /// source Kafka/Service Bus message, never <see cref="Guid.NewGuid"/>) so a redelivered
    /// create targets the same item id both times (cosmos-db.instructions.md §5).
    /// </summary>
    public static InventoryEvent Create(string id, string warehouseId, string sku, int initialQuantity, DateTime nowUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(warehouseId);
        ArgumentException.ThrowIfNullOrWhiteSpace(sku);
        ArgumentOutOfRangeException.ThrowIfNegative(initialQuantity);

        return new InventoryEvent
        {
            Id = id,
            WarehouseId = warehouseId,
            Sku = sku,
            OnHandQuantity = initialQuantity,
            CreatedUtc = nowUtc,
            ModifiedUtc = nowUtc,
        };
    }

    /// <summary>Rehydrates an aggregate from persisted state - the repository mapper's entry point, not for new aggregates.</summary>
    public static InventoryEvent Rehydrate(
        string id,
        string warehouseId,
        string sku,
        int onHandQuantity,
        DateTime createdUtc,
        DateTime modifiedUtc,
        IReadOnlyDictionary<string, int>? activeReservations = null)
    {
        var aggregate = new InventoryEvent
        {
            Id = id,
            WarehouseId = warehouseId,
            Sku = sku,
            OnHandQuantity = onHandQuantity,
            CreatedUtc = createdUtc,
            ModifiedUtc = modifiedUtc,
        };

        if (activeReservations is not null)
        {
            foreach (var (reservationId, quantity) in activeReservations)
            {
                aggregate.reservations[reservationId] = quantity;
            }
        }

        return aggregate;
    }

    /// <summary>
    /// Reserves quantity against on-hand. Idempotent per <paramref name="reservationId"/> - a
    /// redelivered message reserving the same id again is a no-op, not a double-decrement
    /// (dotnet-architecture-good-practices.instructions.md §5).
    /// </summary>
    public void Reserve(string reservationId, int quantity, DateTime nowUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reservationId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(quantity);

        if (reservations.ContainsKey(reservationId))
        {
            return;
        }

        if (quantity > OnHandQuantity)
        {
            throw new InsufficientStockException(WarehouseId, Sku, quantity, OnHandQuantity);
        }

        OnHandQuantity -= quantity;
        reservations[reservationId] = quantity;
        ModifiedUtc = nowUtc;

        RaiseDomainEvent(new StockReserved(Id, WarehouseId, Sku, quantity, reservationId));
    }

    /// <summary>
    /// Converts an active reservation into a firm allocation. On-hand was already decremented at
    /// <see cref="Reserve"/> time, so this does not decrement it again - it only moves the
    /// reservation off the ledger.
    /// </summary>
    public void Allocate(string reservationId, DateTime nowUtc)
    {
        if (!reservations.Remove(reservationId, out var quantity))
        {
            throw new InvalidOperationException($"No active reservation '{reservationId}' to allocate.");
        }

        ModifiedUtc = nowUtc;
        RaiseDomainEvent(new StockAllocated(Id, WarehouseId, Sku, quantity, reservationId));
    }

    /// <summary>Releases a reservation back to on-hand without allocating it. Idempotent - releasing twice is a no-op.</summary>
    public void ReleaseReservation(string reservationId, DateTime nowUtc)
    {
        if (!reservations.Remove(reservationId, out var quantity))
        {
            return;
        }

        OnHandQuantity += quantity;
        ModifiedUtc = nowUtc;
    }

    /// <summary>Direct on-hand correction outside the reserve/allocate flow (e.g. a cycle count).</summary>
    public void Adjust(int newQuantity, string reason, DateTime nowUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(newQuantity);
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);

        var previous = OnHandQuantity;
        OnHandQuantity = newQuantity;
        ModifiedUtc = nowUtc;

        RaiseDomainEvent(new StockAdjusted(Id, WarehouseId, Sku, previous, newQuantity, reason));
    }
}
