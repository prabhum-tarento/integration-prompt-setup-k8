using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Events;

/// <summary>Raised when an internal-hallmarking FINISHED status moves quantity from in-transit into available stock on the target hallmark's <c>ItemStockInventory</c> aggregate (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.5).</summary>
/// <param name="ItemStockInventoryId">Id of the <c>ItemStockInventory</c> aggregate (target hallmark) the transit was completed against.</param>
/// <param name="FulfilmentId">Fulfilment location the completion occurred at.</param>
/// <param name="ItemCode">Item code that completed transit.</param>
/// <param name="Quantity">Quantity moved from <c>InTransit</c> into <c>B2BAvailable</c>.</param>
public sealed record InternalHallmarkingTransitCompleted(
    string ItemStockInventoryId, string FulfilmentId, string ItemCode, int Quantity) : IDomainEvent
{
    /// <inheritdoc />
    public Guid EventId { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime OccurredOnUtc { get; } = DateTime.UtcNow;
}
