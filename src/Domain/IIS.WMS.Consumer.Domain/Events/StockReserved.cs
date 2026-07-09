using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Events;

/// <summary>Raised when quantity is reserved against an inventory aggregate's on-hand balance.</summary>
/// <param name="InventoryEventId">Id of the <c>InventoryEvent</c> aggregate the reservation was taken against.</param>
/// <param name="WarehouseId">Warehouse the reserved stock belongs to.</param>
/// <param name="Sku">SKU the reserved stock belongs to.</param>
/// <param name="Quantity">Quantity moved from on-hand into the reservation.</param>
/// <param name="ReservationId">Id of the reservation created - what a later <c>Allocate</c> or <c>ReleaseReservation</c> call references.</param>
public sealed record StockReserved(
    string InventoryEventId, string WarehouseId, string Sku, int Quantity, string ReservationId) : IDomainEvent
{
    /// <inheritdoc />
    public Guid EventId { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime OccurredOnUtc { get; } = DateTime.UtcNow;
}
