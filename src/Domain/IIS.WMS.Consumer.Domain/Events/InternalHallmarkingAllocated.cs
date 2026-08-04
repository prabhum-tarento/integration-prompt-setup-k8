using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Events;

/// <summary>Raised when an internal-hallmarking STARTED status allocates B2B quantity against an <c>ItemStockInventory</c> aggregate (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.1).</summary>
/// <param name="ItemStockInventoryId">Id of the <c>ItemStockInventory</c> aggregate that was allocated against.</param>
/// <param name="FulfilmentId">Fulfilment location the allocation occurred at.</param>
/// <param name="ItemCode">Item code that was allocated.</param>
/// <param name="Quantity">Signed quantity applied to <c>B2BAllocated</c>.</param>
public sealed record InternalHallmarkingAllocated(
    string ItemStockInventoryId, string FulfilmentId, string ItemCode, int Quantity) : IDomainEvent
{
    /// <inheritdoc />
    public Guid EventId { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime OccurredOnUtc { get; } = DateTime.UtcNow;
}
