using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.OrderTracking;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Egress;

/// <inheritdoc cref="IOrderTrackingPublisher"/>
internal sealed class OrderTrackingPublisher(
    IServiceBusRelayPublisher relayPublisher,
    IOptions<InventoryPublishOptions> publishOptions,
    ICorrelationContext correlationContext,
    ILogger<OrderTrackingPublisher> logger) : IOrderTrackingPublisher
{
    private const string EventTypeName = "OrderTrackingCommonRequest";

    /// <inheritdoc/>
    public async Task PublishAsync(OrderTrackingRelayRequest request, CancellationToken cancellationToken = default)
    {
        var json = JsonSerializer.Serialize(request);

        var relayMessage = new ServiceBusRelayMessage(
            QueueName: publishOptions.Value.OrderTrackingQueueName,
            SessionId: request.ReferenceId,
            MessageId: $"{request.ReferenceId}:{EventTypeName}",
            CorrelationId: correlationContext.CorrelationId,
            AppId: correlationContext.AppId,
            Types: [EventTypeName],
            SourceName: nameof(OrderTrackingPublisher),
            PayloadName: EventTypeName,
            Json: json);

        try
        {
            await relayPublisher.PublishAsync(relayMessage, cancellationToken);

            logger.LogInformation(
                "Published §3.9 order-tracking request for ReferenceId {ReferenceId}, OrderId {OrderId}, OrderStatus {OrderStatus} to queue {QueueName}.",
                request.ReferenceId, request.OrderId, request.OrderStatus, relayMessage.QueueName);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Best-effort side channel (docs/events/inventory.InventoryStateChanged.md §3.9/§8) - a
            // publish failure here must not fail the message's overall inventory outcome.
            logger.LogError(
                ex,
                "Failed to publish §3.9 order-tracking request for ReferenceId {ReferenceId}, OrderId {OrderId} to queue {QueueName} - continuing without it.",
                request.ReferenceId, request.OrderId, relayMessage.QueueName);
        }
    }
}
