namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged.Handlers;

/// <summary>
/// Business logic for one message off the "order-status-changed" queue - resolved from a fresh DI
/// scope per message by
/// <see cref="OrderStatusChangedServiceBusHostedService.ProcessMessageAsync"/>, mirroring
/// <see cref="InternalHallmarkingStatusChanged.Handlers.IInternalHallmarkingStatusChangedHandler"/>.
/// </summary>
public interface IOrderStatusChangedHandler
{
    /// <summary>Classifies the warehouse, selects the reference id, maps the status, normalizes the fulfilment-unit id, and publishes the resulting order-tracking request.</summary>
    /// <param name="message">The deserialized inbound event.</param>
    /// <param name="correlationId">This message's resolved correlation id.</param>
    /// <param name="cancellationToken">Token to cancel the publish.</param>
    Task HandleAsync(OrderStatusChangedEvent message, string correlationId, CancellationToken cancellationToken);
}
