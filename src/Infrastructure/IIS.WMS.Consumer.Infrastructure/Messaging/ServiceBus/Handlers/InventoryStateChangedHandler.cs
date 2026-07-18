using IIS.WMS.Common.Messaging;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus.Handlers;

/// <summary>
/// Dispatches an inbound inventory event to the matching <see cref="IInventoryEventService"/> use case
/// by <see cref="InboundInventoryEventMessage.EventType"/> - migrated from the old sealed
/// <c>ServiceBusConsumerHostedService.DispatchEventAsync</c>, unchanged in behavior. An unknown event
/// type throws, so <c>ServiceBusConsumerHostedService{TMessage}</c>'s processing try/catch maps
/// it to <c>DeadLettered</c> like any other processing failure.
/// </summary>
/// <param name="inventoryEventService">
/// Kept as the layering boundary this handler calls through, exactly like <c>InventoryEventsController</c>
/// does - never the repository directly. A future repository-level customization (e.g. a bespoke
/// read-modify-write not covered by the existing use cases) would be added as a new
/// <see cref="IInventoryEventService"/> method, not by bypassing it here.
/// </param>
/// <param name="logger">Logger for unknown-event-type warnings.</param>
public sealed class InventoryStateChangedHandler(IInventoryEventService inventoryEventService, ILogger<InventoryStateChangedHandler> logger)
    : IInventoryStateChangedHandler
{
    /// <inheritdoc/>
    public async Task HandleAsync(InboundInventoryEventMessage message, string correlationId, CancellationToken cancellationToken)
    {
        switch (message.EventType)
        {
            case "Create":
                await inventoryEventService.CreateAsync(
                    new CreateInventoryEventRequest(message.WarehouseId, message.Sku, message.Quantity),
                    cancellationToken);
                break;

            case "Reserve":
                await inventoryEventService.ReserveStockAsync(
                    message.WarehouseId, message.Sku,
                    new ReserveStockRequest(message.EventId, message.Quantity),
                    cancellationToken);
                break;

            default:
                logger.LogWarning(
                    "Unknown inventory event type '{EventType}' for CorrelationId {CorrelationId}.",
                    message.EventType, correlationId);

                throw new InvalidOperationException($"Unknown inventory event type '{message.EventType}'.");
        }

        // TODO(ai): point #10(b) - no target queue/mapping was specified for a downstream relay of this
        // event. Once one is, build the mapped outbound message here and send it via
        // IServiceBusRelayPublisher.PublishAsync(new ServiceBusRelayMessage(...), cancellationToken).
    }
}
