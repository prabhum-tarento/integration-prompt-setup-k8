using IIS.WMS.Common.Messaging;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus.Handlers;

/// <summary>
/// Business logic for one message off the "inventory-state-changed" queue - resolved from a fresh DI
/// scope per message by <see cref="InventoryStateChangedServiceBusHostedService.ProcessMessageAsync"/>, the
/// same split Kafka's schema handlers use to keep transport plumbing out of the use-case logic itself.
/// </summary>
public interface IInventoryStateChangedHandler
{
    /// <summary>Applies <paramref name="message"/> to the inventory aggregate.</summary>
    /// <param name="message">The deserialized inbound event.</param>
    /// <param name="correlationId">This message's resolved correlation id.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task HandleAsync(InboundInventoryEventMessage message, string correlationId, CancellationToken cancellationToken);
}
