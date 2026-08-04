using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Egress;

/// <inheritdoc cref="IInventoryAdjustedReflexPublisher"/>
internal sealed class InventoryAdjustedReflexPublisher(
    IServiceBusRelayPublisher relayPublisher,
    IOptions<InventoryPublishOptions> publishOptions,
    ICorrelationContext correlationContext,
    ILogger<InventoryAdjustedReflexPublisher> logger) : IInventoryAdjustedReflexPublisher
{
    private const string EventTypeName = "Inventory_InternalHallmarkingInventoryAdjusted";

    /// <inheritdoc/>
    public async Task PublishAsync(
        string channel,
        string id,
        DateTime adjustmentDate,
        string locationId,
        string locationType,
        string? entity,
        string itemCode,
        int quantity,
        string countryOfOrigin,
        string hallmarkTo,
        State toState,
        Status toStatus,
        string referenceId,
        CancellationToken cancellationToken = default)
    {
        var request = new InventoryAdjustedReflexPublishRequest
        {
            Channel = channel,
            Id = id,
            AdjustmentDate = adjustmentDate,
            Location = new PublishLocation(locationId, locationType),
            Entity = entity,
            ItemCode = itemCode,
            Quantity = quantity,
            CountryOfOrigin = countryOfOrigin,
            HallmarkTo = hallmarkTo,
            ToState = new PublishStateSnapshot(toState.ToString(), toStatus.ToString()),
            ReferenceId = referenceId,
        };

        var json = JsonSerializer.Serialize(request);

        var relayMessage = new ServiceBusRelayMessage(
            QueueName: publishOptions.Value.InventoryAdjustedReflexQueueName,
            SessionId: referenceId,
            MessageId: $"{referenceId}:{EventTypeName}",
            CorrelationId: correlationContext.CorrelationId,
            AppId: correlationContext.AppId,
            Types: [EventTypeName],
            SourceName: nameof(InventoryAdjustedReflexPublisher),
            PayloadName: EventTypeName,
            Json: json);

        await relayPublisher.PublishAsync(relayMessage, cancellationToken);

        logger.LogInformation(
            "Published §3.5 internal-hallmarking inventory-adjusted event for Id {Id}, ItemCode {ItemCode} to queue {QueueName}.",
            id, itemCode, relayMessage.QueueName);
    }
}
