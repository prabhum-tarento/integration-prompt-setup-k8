using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Domain.Events;

/// <summary>
/// Raised when an order allocates stock to an <c>ItemStockInventory</c> aggregate
/// (docs/events/inventory.OrderToInventoryAllocated.md §3.2/§3.3).
/// </summary>
/// <param name="ItemStockInventoryId">Id of the <c>ItemStockInventory</c> aggregate that was allocated against.</param>
/// <param name="FulfilmentId">Fulfilment location the allocation occurred at.</param>
/// <param name="ItemCode">Item code that was allocated.</param>
/// <param name="OrderDomain">Business domain the allocation targets (B2B/B2C/InternalHallmarking/ExternalHallmarking).</param>
/// <param name="AllocatedFromB2BBucketQuantity">Quantity allocated from the B2B bucket.</param>
/// <param name="AllocatedFromB2CBucketQuantity">Quantity allocated from the B2C bucket.</param>
public sealed record OrderAllocatedToInventory(
    string ItemStockInventoryId,
    string FulfilmentId,
    string ItemCode,
    OrderDomain OrderDomain,
    int AllocatedFromB2BBucketQuantity,
    int AllocatedFromB2CBucketQuantity) : IDomainEvent
{
    /// <inheritdoc />
    public Guid EventId { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime OccurredOnUtc { get; } = DateTime.UtcNow;
}
