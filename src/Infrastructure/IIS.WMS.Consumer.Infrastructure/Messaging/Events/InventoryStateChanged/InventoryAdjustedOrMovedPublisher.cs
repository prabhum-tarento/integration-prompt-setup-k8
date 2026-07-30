using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

/// <inheritdoc cref="IInventoryAdjustedOrMovedPublisher"/>
internal sealed class InventoryAdjustedOrMovedPublisher(
    IServiceBusRelayPublisher relayPublisher,
    IOptions<InventoryPublishOptions> publishOptions,
    ICorrelationContext correlationContext,
    ILogger<InventoryAdjustedOrMovedPublisher> logger) : IInventoryAdjustedOrMovedPublisher
{
    private const string EventTypeName = "Inventory_B2BInventoryAdjustedOrMoved";

    /// <inheritdoc/>
    public async Task PublishAsync(
        string channel,
        string id,
        DateTime adjustmentDate,
        string locationId,
        string locationType,
        string? entity,
        State fromState,
        Status fromStatus,
        State toState,
        Status toStatus,
        string? referenceId,
        IReadOnlyList<InventoryAdjustedOrMovedLine> lines,
        CancellationToken cancellationToken = default)
    {
        // Fix SAE-2798: skip the publish entirely when neither side of the transition changed state and
        // neither is AVAILABLE, unless the correlation context already declares this a B2B_INVENTORY_ADJUSTED
        // redelivery (Reflex's inventoryAdjustedOrMovedEventHandlerAsync).
        var isRedelivery = correlationContext.Types.Contains(KafkaEvents.InventoryAdjustedEventType);
        if (!isRedelivery && fromState == toState && fromState != State.AVAILABLE)
        {
            logger.LogInformation(
                "Skipping §3.6 B2B adjusted/moved publish for Id {Id} - FromState and ToState are both {State} " +
                "and this isn't a B2B_INVENTORY_ADJUSTED redelivery (SAE-2798).", id, fromState);

            return;
        }

        // Fix SAE-3032: force the outbound status to UNKNOWN for whichever side isn't AVAILABLE - the
        // caller's own fromStatus/toStatus values are never mutated.
        var outboundFromStatus = fromState == State.AVAILABLE ? fromStatus : Status.UNKNOWN;
        var outboundToStatus = toState == State.AVAILABLE ? toStatus : Status.UNKNOWN;

        var resolvedReferenceId = string.IsNullOrWhiteSpace(referenceId) ? Guid.NewGuid().ToString() : referenceId;

        var request = new InventoryAdjustedOrMovedPublishRequest
        {
            Channel = channel,
            Id = id,
            AdjustmentDate = adjustmentDate,
            Location = new PublishLocation(locationId, locationType),
            Entity = entity,
            FromState = new PublishStateSnapshot(fromState.ToString(), outboundFromStatus.ToString()),
            ToState = new PublishStateSnapshot(toState.ToString(), outboundToStatus.ToString()),
            ReferenceId = resolvedReferenceId,
            InventoryEventType = KafkaEvents.InventoryAdjustedEventType,
            Lines = [.. lines.Select(line => new InventoryAdjustedOrMovedLine
            {
                ItemCode = line.ItemCode,
                Qty = Math.Abs(line.Qty),
                CountryOfOrigin = line.CountryOfOrigin,
                Hallmarking = line.Hallmarking,
                Reason = line.Reason,
            })],
        };

        var json = JsonSerializer.Serialize(request);

        var relayMessage = new ServiceBusRelayMessage(
            QueueName: publishOptions.Value.SapAdjustedOrMovedQueueName,
            SessionId: id,
            MessageId: $"{id}:{EventTypeName}",
            CorrelationId: correlationContext.CorrelationId,
            AppId: correlationContext.AppId,
            Types: [EventTypeName],
            SourceName: nameof(InventoryAdjustedOrMovedPublisher),
            PayloadName: EventTypeName,
            Json: json);

        await relayPublisher.PublishAsync(relayMessage, cancellationToken);

        logger.LogInformation(
            "Published §3.6 B2B adjusted/moved event for Id {Id}, ReferenceId {ReferenceId} to queue {QueueName}.",
            id, resolvedReferenceId, relayMessage.QueueName);
    }
}
