using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Events;

/// <summary>Raised when a pick is applied against an <c>ItemStockInventory</c> aggregate's allocated/prepared quantities.</summary>
/// <param name="ItemStockInventoryId">Id of the <c>ItemStockInventory</c> aggregate that was picked.</param>
/// <param name="FulfilmentId">Fulfilment location the pick occurred at.</param>
/// <param name="ItemCode">Item code that was picked.</param>
/// <param name="Channel">Either <c>"B2B"</c> or <c>"B2C"</c> - which allocated/prepared pair was moved.</param>
/// <param name="Quantity">Quantity moved from allocated into prepared.</param>
/// <param name="WasClamped">Whether the allocated quantity would have gone negative and was clamped to zero instead - a data-drift warning signal, not a rejection.</param>
public sealed record ItemStockPicked(
    string ItemStockInventoryId, string FulfilmentId, string ItemCode, string Channel, int Quantity, bool WasClamped) : IDomainEvent
{
    /// <inheritdoc />
    public Guid EventId { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime OccurredOnUtc { get; } = DateTime.UtcNow;
}
