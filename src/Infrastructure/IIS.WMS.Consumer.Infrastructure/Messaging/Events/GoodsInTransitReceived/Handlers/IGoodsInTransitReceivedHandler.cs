namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived.Handlers;

/// <summary>
/// Business logic for one message off the "goods-in-transit-received" queue - resolved from a fresh DI
/// scope per message by
/// <see cref="GoodsInTransitReceivedServiceBusHostedService.ProcessMessageAsync"/>, mirroring
/// <see cref="OrderStatusChanged.Handlers.IOrderStatusChangedHandler"/>.
/// </summary>
public interface IGoodsInTransitReceivedHandler
{
    /// <summary>Applies every shipment line's inventory update, publishes the OMS delta when eligible, and publishes the order-tracking request.</summary>
    /// <param name="message">The deserialized inbound event.</param>
    /// <param name="correlationId">This message's resolved correlation id.</param>
    /// <param name="cancellationToken">Token to cancel processing.</param>
    Task HandleAsync(GoodsInTransitReceivedEvent message, string correlationId, CancellationToken cancellationToken);
}
