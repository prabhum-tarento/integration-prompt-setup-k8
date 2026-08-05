namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped.Handlers;

/// <summary>
/// Business logic for one message off the "consolidated-order-shipped" queue - resolved from a fresh DI
/// scope per message by
/// <see cref="ConsolidatedOrderShippedServiceBusHostedService.ProcessMessageAsync"/>, mirroring
/// <see cref="OrderStatusChanged.Handlers.IOrderStatusChangedHandler"/>.
/// </summary>
public interface IConsolidatedOrderShippedHandler
{
    /// <summary>
    /// Confirms B2B inventory per shipment line, recalculates B2C extension, publishes the OMS delta and
    /// ICR snapshot, runs the e-commerce engraving workflow, and publishes the resulting order-tracking
    /// request(s) (docs/events/b2b.sales.ConsolidatedOrderShipped.md §2/§3).
    /// </summary>
    /// <param name="message">The deserialized inbound event.</param>
    /// <param name="correlationId">This message's resolved correlation id.</param>
    /// <param name="cancellationToken">Token to cancel processing.</param>
    Task HandleAsync(ConsolidatedOrderShippedEvent message, string correlationId, CancellationToken cancellationToken);
}
