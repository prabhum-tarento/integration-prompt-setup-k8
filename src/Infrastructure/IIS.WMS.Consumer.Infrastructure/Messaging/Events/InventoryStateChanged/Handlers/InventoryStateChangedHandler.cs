using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Handlers;

/// <summary>
/// Applies one relayed <see cref="InventoryStateChangedEvent"/> - ported from the upstream Reflex
/// facade's <c>InventoryStateChangedQueueTrigger</c>, excluding its Durable Functions
/// <c>InventoryStateChangedOrchestrator</c>/Activity Trigger dispatch (this service has no Durable Task
/// engine; the Kafka-to-Service-Bus relay pipeline itself, running as its own KEDA-scaled AKS Deployment
/// per kubernetes-deployment-best-practices.instructions.md, is this service's equivalent
/// durability/retry mechanism - see docs/InventoryStateChanged-OrderTracking-Relay.md). Detects a
/// pick/unpick transition, applies the corresponding <see cref="ItemStockInventory"/> mutation per item
/// line via <see cref="IItemStockInventoryService"/> (ported from Reflex's
/// <c>InventoryPickEventHandler</c>/<c>InventoryUnpickEventHandler</c>), and builds the corresponding
/// <see cref="OrderTrackingRelayRequest"/>, exactly as the reference trigger does.
/// </summary>
/// <param name="itemStockInventoryService">Applies the actual pick/unpick quantity mutation.</param>
/// <param name="logger">Logger for the OrderTracking relay's disabled-state notice and unpick-type rejects.</param>
public sealed class InventoryStateChangedHandler(
    IItemStockInventoryService itemStockInventoryService, ILogger<InventoryStateChangedHandler> logger)
    : IInventoryStateChangedHandler
{
    /// <inheritdoc/>
    public async Task HandleAsync(InventoryStateChangedEvent message, string correlationId, CancellationToken cancellationToken)
    {
        var isPickEvent = InventoryStateTransitionRules.IsPickableToPrepared(message);
        var isUnpickEvent = InventoryStateTransitionRules.IsUnpickTransition(message);
        await ApplyItemStockMutationsAsync(message, isPickEvent, correlationId, cancellationToken);

        if (!isPickEvent && !isUnpickEvent)
        {
            return;
        }


        var request = new OrderTrackingRelayRequest(
            ReferenceId: message.Id,
            Channel: message.Channel.ToString(),
            FulfilmentUnitId: message.Location.Id,
            FulfilmentUnitType: message.Location.Type.ToString(),
            FunctionName: nameof(InventoryStateChangedHandler),
            OrderId: message.ReferenceId,
            OrderStatus: OrderTrackingStatus.PICKED,
            OrderType: (message.Type == InventoryEventChangeType.PickedB2C ? OrderType.SALES : OrderType.TRANSFER).ToString(),
            Lines: [.. message.ItemLines.Select(item => new OrderTrackingRelayLine(
                ItemCode: item.ProductId,
                CountryOfOrigin: item.CountryOfOrigin,
                HallMarkType: item.Hallmarking,
                Qty: item.Quantity))]);

        // TODO(ai): no OrderTracking Service Bus queue is configured anywhere in this repo yet (mirrors
        // the upstream Reflex trigger, whose own downstream send is likewise commented out today). Once
        // one is defined, publish via IServiceBusRelayPublisher.PublishAsync(new ServiceBusRelayMessage(...),
        // cancellationToken) instead of only logging - see docs/InventoryStateChanged-OrderTracking-Relay.md.
        logger.LogInformation(
            "OrderTracking relay is disabled - no target queue configured. Would have relayed {OrderStatus}/{OrderType} " +
            "for ReferenceId {ReferenceId}, OrderId {OrderId}, FulfilmentUnitId {FulfilmentUnitId}, CorrelationId {CorrelationId}.",
            request.OrderStatus, request.OrderType, request.ReferenceId, request.OrderId, request.FulfilmentUnitId, correlationId);
    }

    /// <summary>
    /// Applies the <see cref="ItemStockInventory"/> mutation for each item line - mirrors Reflex's
    /// orchestrator's own per-item-line loop (<c>InventoryStateChangedOrchestrator.cs</c>). A pick
    /// dispatches on <see cref="InventoryEventChangeType.PickedB2B"/>/<see cref="InventoryEventChangeType.PickedB2C"/>;
    /// an unpick only applies for <see cref="InventoryEventChangeType.Dgp"/> (mirrors Reflex's
    /// <c>InventoryChangeType.DGP</c> guard) - any other type on an unpick transition is logged and
    /// skipped, matching Reflex's "Invalid Type" reject.
    /// </summary>
    private async Task ApplyItemStockMutationsAsync(
        InventoryStateChangedEvent message, bool isPickEvent, string correlationId, CancellationToken cancellationToken)
    {
        if (isPickEvent)
        {
            if (message.Type is not (InventoryEventChangeType.PickedB2B or InventoryEventChangeType.PickedB2C))
            {
                logger.LogWarning(
                    "Pick transition for ReferenceId {ReferenceId}, CorrelationId {CorrelationId} has unsupported Type {Type} - skipping stock mutation.",
                    message.Id, correlationId, message.Type);

                return;
            }

            var channel = message.Type == InventoryEventChangeType.PickedB2B ? ItemStockPickChannel.B2B : ItemStockPickChannel.B2C;

            foreach (var item in message.ItemLines)
            {
                await itemStockInventoryService.ApplyPickAsync(
                    message.Location.Id, item.ProductId, item.CountryOfOrigin, item.Hallmarking,
                    channel, item.Quantity, cancellationToken);
            }

            return;
        }

        if (message.Type != InventoryEventChangeType.Dgp)
        {
            logger.LogWarning(
                "Unpick transition for ReferenceId {ReferenceId}, CorrelationId {CorrelationId} has unsupported Type {Type} - skipping stock mutation.",
                message.Id, correlationId, message.Type);

            return;
        }

        foreach (var item in message.ItemLines)
        {
            await itemStockInventoryService.ApplyUnpickAsync(
                message.Location.Id, item.ProductId, item.CountryOfOrigin, item.Hallmarking,
                item.Quantity, cancellationToken);
        }

        // TODO(ai): Reflex's IsExtended branch (both pick and unpick) additionally calls
        // CalculateB2CExtensionAsync to recalculate B2CExtended/B2CAVL and emit an OMS delta - not
        // ported here since it depends on IItemLevelSegmentationRepository/
        // IFulfilmentLevelSegmentationRepository, neither of which exists in this repo. See
        // docs/InventoryStateChanged-OrderTracking-Relay.md.
    }
}
