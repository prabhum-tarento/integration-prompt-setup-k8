using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Events;

/// <summary>Raised when an internal-hallmarking PICKED status applies a consolidated shipment against an <c>ItemStockInventory</c> aggregate (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.3).</summary>
/// <param name="ItemStockInventoryId">Id of the <c>ItemStockInventory</c> aggregate the shipment was applied against.</param>
/// <param name="FulfilmentId">Fulfilment location the shipment occurred at.</param>
/// <param name="ItemCode">Item code that was shipped.</param>
/// <param name="ConfirmationType">Which §3.3 branch (<c>PRELIMINARY</c>/<c>STANDARD_FOLLOWING_PRELIMINARY</c>/other) was applied.</param>
/// <param name="ShippedQuantity">Quantity shipped.</param>
public sealed record InternalHallmarkingShipped(
    string ItemStockInventoryId, string FulfilmentId, string ItemCode, string ConfirmationType, int ShippedQuantity) : IDomainEvent
{
    /// <inheritdoc />
    public Guid EventId { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime OccurredOnUtc { get; } = DateTime.UtcNow;
}
