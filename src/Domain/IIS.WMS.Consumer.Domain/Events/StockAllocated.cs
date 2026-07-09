using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Events;

/// <summary>Raised when a prior reservation is converted into a firm allocation (e.g. an order ships).</summary>
/// <param name="InventoryEventId">Id of the <c>InventoryEvent</c> aggregate the allocation was made against.</param>
/// <param name="WarehouseId">Warehouse the allocated stock belongs to.</param>
/// <param name="Sku">SKU the allocated stock belongs to.</param>
/// <param name="Quantity">Quantity that was reserved and is now allocated.</param>
/// <param name="ReservationId">Id of the reservation that was allocated - the same id <see cref="StockReserved"/> raised.</param>
public sealed record StockAllocated(
    string InventoryEventId, string WarehouseId, string Sku, int Quantity, string ReservationId) : IDomainEvent
{
    /// <inheritdoc />
    public Guid EventId { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime OccurredOnUtc { get; } = DateTime.UtcNow;
}
