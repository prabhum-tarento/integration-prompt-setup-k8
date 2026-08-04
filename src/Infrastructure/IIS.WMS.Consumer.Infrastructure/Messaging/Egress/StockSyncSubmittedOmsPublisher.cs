using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Egress;

/// <inheritdoc cref="IStockSyncSubmittedOmsPublisher"/>
internal sealed class StockSyncSubmittedOmsPublisher(
    IServiceBusRelayPublisher relayPublisher,
    IOptions<InventoryPublishOptions> publishOptions,
    ICorrelationContext correlationContext,
    ILogger<StockSyncSubmittedOmsPublisher> logger) : IStockSyncSubmittedOmsPublisher
{
    private const string EventTypeName = "Inventory_B2CStockSyncSubmitted";
    private const string BrMarket = "BR";
    private const string CaMarket = "CA";

    /// <inheritdoc/>
    public async Task PublishAsync(
        string fulfilmentId,
        string locationType,
        string itemCode,
        int b2cAvailableQuantity,
        string eventId,
        CancellationToken cancellationToken = default)
    {
        var market = fulfilmentId == FulfilmentLocationIds.BrzDc3PlFulfilmentId ? BrMarket : CaMarket;
        var reportedLocationId = fulfilmentId == FulfilmentLocationIds.BrzDc3PlFulfilmentId
            ? FulfilmentLocationIds.Brz3PlConsigneeId
            : fulfilmentId;

        var request = new StockSyncSubmittedOmsPublishRequest
        {
            ProductId = itemCode,
            ProductUnits = "N/A",
            Location = new PublishLocation(reportedLocationId, locationType),
            Market = market,
            QuantityDetails =
            [
                new StockSyncSubmittedOmsQuantityDetail
                {
                    Quantity = b2cAvailableQuantity,
                    State = State.AVAILABLE.ToString(),
                    Status = Status.PICKABLE.ToString(),
                },
            ],
        };

        var json = JsonSerializer.Serialize(request);
        var referenceId = $"{fulfilmentId}:{itemCode}:{eventId}";

        var relayMessage = new ServiceBusRelayMessage(
            QueueName: publishOptions.Value.B2CStockQueueName,
            SessionId: referenceId,
            MessageId: $"{referenceId}:{EventTypeName}",
            CorrelationId: correlationContext.CorrelationId,
            AppId: correlationContext.AppId,
            Types: [EventTypeName],
            SourceName: nameof(StockSyncSubmittedOmsPublisher),
            PayloadName: EventTypeName,
            Json: json);

        await relayPublisher.PublishAsync(relayMessage, cancellationToken);

        logger.LogInformation(
            "Published §3.5 OMS B2C stock snapshot for ProductId {ProductId}, Market {Market} to queue {QueueName}.",
            itemCode, market, relayMessage.QueueName);
    }
}
