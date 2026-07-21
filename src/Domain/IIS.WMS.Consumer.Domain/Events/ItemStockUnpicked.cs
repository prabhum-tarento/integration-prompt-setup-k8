using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Events;

/// <summary>Raised when an unpick (<c>Dgp</c>) is applied against an <c>ItemStockInventory</c> aggregate's B2B prepared quantity.</summary>
/// <param name="ItemStockInventoryId">Id of the <c>ItemStockInventory</c> aggregate that was unpicked.</param>
/// <param name="FulfilmentId">Fulfilment location the unpick occurred at.</param>
/// <param name="ItemCode">Item code that was unpicked.</param>
/// <param name="Quantity">Quantity moved out of B2B prepared.</param>
public sealed record ItemStockUnpicked(
    string ItemStockInventoryId, string FulfilmentId, string ItemCode, int Quantity) : IDomainEvent
{
    /// <inheritdoc />
    public Guid EventId { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime OccurredOnUtc { get; } = DateTime.UtcNow;
}
