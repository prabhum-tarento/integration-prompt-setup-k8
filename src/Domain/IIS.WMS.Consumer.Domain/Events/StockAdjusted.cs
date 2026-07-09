using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Events;

/// <summary>Raised for a direct on-hand quantity correction outside the reserve/allocate flow (e.g. a cycle count).</summary>
/// <param name="InventoryEventId">Id of the <c>InventoryEvent</c> aggregate that was adjusted.</param>
/// <param name="WarehouseId">Warehouse the adjusted stock belongs to.</param>
/// <param name="Sku">SKU the adjusted stock belongs to.</param>
/// <param name="PreviousQuantity">On-hand quantity immediately before the adjustment.</param>
/// <param name="NewQuantity">On-hand quantity immediately after the adjustment.</param>
/// <param name="Reason">Free-text reason for the correction (e.g. "cycle count"), for audit purposes.</param>
public sealed record StockAdjusted(
    string InventoryEventId, string WarehouseId, string Sku, int PreviousQuantity, int NewQuantity, string Reason) : IDomainEvent
{
    /// <inheritdoc />
    public Guid EventId { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime OccurredOnUtc { get; } = DateTime.UtcNow;
}
