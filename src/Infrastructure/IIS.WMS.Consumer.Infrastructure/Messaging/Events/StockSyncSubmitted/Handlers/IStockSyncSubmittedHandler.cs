namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockSyncSubmitted.Handlers;

/// <summary>
/// Applies one relayed <see cref="StockSyncSubmittedEvent"/> (docs/events/inventory.StockSyncSubmitted.md).
/// The sole consumer of the "stock-sync-submitted" Service Bus queue.
/// </summary>
public interface IStockSyncSubmittedHandler
{
    Task HandleAsync(StockSyncSubmittedEvent message, string correlationId, CancellationToken cancellationToken);
}
