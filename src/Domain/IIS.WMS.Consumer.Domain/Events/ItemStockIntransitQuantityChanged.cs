using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Events;

/// <summary>Raised when an <c>ItemStockIntransit</c> aggregate's quantity is moved as part of a status transition (docs/events/inventory.InternalHallmarkingStatusChanged.md §5.2/§6).</summary>
/// <param name="ItemStockIntransitId">Id of the <c>ItemStockIntransit</c> aggregate that changed.</param>
/// <param name="ItemCode">Item code the transit record tracks.</param>
/// <param name="Status">The transit status (<c>ALLOCATED</c>/<c>PICKED</c>/<c>INTRANSIT</c>/<c>CREATED</c>) this record represents.</param>
/// <param name="SignedQuantity">Signed delta applied to the transit quantity - positive on increase, negative on decrease.</param>
public sealed record ItemStockIntransitQuantityChanged(
    string ItemStockIntransitId, string ItemCode, string Status, int SignedQuantity) : IDomainEvent
{
    /// <inheritdoc />
    public Guid EventId { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime OccurredOnUtc { get; } = DateTime.UtcNow;
}
