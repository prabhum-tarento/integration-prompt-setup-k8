using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Events;

/// <summary>Raised when a §3.2 stock-sync Set is applied against an <c>ItemStockInventory</c> aggregate's B2C sellable quantities.</summary>
/// <param name="ItemStockInventoryId">Id of the <c>ItemStockInventory</c> aggregate that was synced.</param>
/// <param name="FulfilmentId">Fulfilment location the sync was reported against.</param>
/// <param name="ItemCode">Item code that was synced.</param>
/// <param name="B2CAvailable">The new (Set, not incremented) B2C available/pickable quantity.</param>
/// <param name="B2CPrepared">The new (Set, not incremented) B2C prepared quantity.</param>
/// <param name="B2CAvailableToSell">The new (Set, not incremented) BR-only available-to-sell quantity, or <see langword="null"/> when not applicable.</param>
public sealed record ItemStockSyncApplied(
    string ItemStockInventoryId, string FulfilmentId, string ItemCode, int B2CAvailable, int B2CPrepared, int? B2CAvailableToSell) : IDomainEvent
{
    /// <inheritdoc />
    public Guid EventId { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime OccurredOnUtc { get; } = DateTime.UtcNow;
}
