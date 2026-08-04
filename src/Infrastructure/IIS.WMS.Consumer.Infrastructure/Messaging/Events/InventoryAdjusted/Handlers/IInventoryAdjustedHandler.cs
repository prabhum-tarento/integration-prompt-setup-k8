using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryAdjusted.Handlers;

/// <summary>
/// Business logic for one message off the "inventory-adjusted" queue - resolved from a fresh DI
/// scope per message by <see cref="InventoryAdjustedServiceBusHostedService.ProcessMessageAsync"/>,
/// mirroring <see cref="InventoryStateChanged.Handlers.IInventoryStateChangedHandler"/>.
/// </summary>
public interface IInventoryAdjustedHandler
{
    /// <summary>Applies <paramref name="message"/> to the inventory aggregate.</summary>
    /// <param name="message">The deserialized inbound event.</param>
    /// <param name="correlationId">This message's resolved correlation id.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task HandleAsync(InventoryAdjustedEvent message, string correlationId, CancellationToken cancellationToken);
}
