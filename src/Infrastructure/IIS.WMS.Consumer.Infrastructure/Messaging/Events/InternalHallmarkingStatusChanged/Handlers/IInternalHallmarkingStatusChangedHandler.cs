namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged.Handlers;

/// <summary>
/// Business logic for one message off the "internal-hallmarking-status-changed" queue - resolved from
/// a fresh DI scope per message by
/// <see cref="InternalHallmarkingStatusChangedServiceBusHostedService.ProcessMessageAsync"/>, mirroring
/// <see cref="InventoryStateChanged.Handlers.IInventoryStateChangedHandler"/>.
/// </summary>
public interface IInternalHallmarkingStatusChangedHandler
{
    /// <summary>Applies <paramref name="message"/>'s status transition and runs its downstream publishes.</summary>
    /// <param name="message">The deserialized inbound event.</param>
    /// <param name="correlationId">This message's resolved correlation id.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task HandleAsync(InternalHallmarkingStatusChangedEvent message, string correlationId, CancellationToken cancellationToken);
}
