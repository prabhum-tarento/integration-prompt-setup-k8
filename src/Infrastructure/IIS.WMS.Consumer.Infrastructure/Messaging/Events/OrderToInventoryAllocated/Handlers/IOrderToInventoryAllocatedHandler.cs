using IIS.WMS.Common.Messaging;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderToInventoryAllocated.Handlers;

/// <summary>
/// Business logic for one message off the "order-to-inventory-allocated" queue - resolved from a fresh DI
/// scope per message by <see cref="OrderToInventoryAllocatedServiceBusHostedService.ProcessMessageAsync"/>.
/// </summary>
public interface IOrderToInventoryAllocatedHandler
{
    /// <summary>Applies <paramref name="message"/> to the inventory aggregate.</summary>
    /// <param name="message">The deserialized inbound event.</param>
    /// <param name="correlationId">This message's resolved correlation id.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task HandleAsync(OrderToInventoryAllocatedEvent message, string correlationId, CancellationToken cancellationToken);
}
