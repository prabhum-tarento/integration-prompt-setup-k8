namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockOnHandUpdated.Handlers;

/// <summary>
/// Applies one relayed <see cref="StockOnHandUpdatedEvent"/> (docs/events/inventory.StockOnHandUpdated.md).
/// The sole consumer of the "stock-on-hand-updated" Service Bus queue.
/// </summary>
public interface IStockOnHandUpdatedHandler
{
    Task HandleAsync(StockOnHandUpdatedEvent message, string correlationId, CancellationToken cancellationToken);
}
