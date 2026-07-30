using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

/// <inheritdoc cref="IDeltaTowardsOmsPublisher"/>
internal sealed class DeltaTowardsOmsPublisher(
    IFulfilmentUnitRepository fulfilmentUnitRepository,
    IServiceBusRelayPublisher relayPublisher,
    IOptions<InventoryPublishOptions> publishOptions,
    ICorrelationContext correlationContext,
    TimeProvider timeProvider,
    ILogger<DeltaTowardsOmsPublisher> logger) : IDeltaTowardsOmsPublisher
{
    private const string EventTypeName = "Inventory_B2CInventoryAdjusted";
    private const string UnknownMarket = "UNKNOWN";

    /// <inheritdoc/>
    public async Task PublishAsync(
        string productId,
        string locationId,
        string locationType,
        string countryOfOrigin,
        string hallmarking,
        int deltaTowardsOms,
        CancellationToken cancellationToken = default)
    {
        var fulfilmentUnit = await fulfilmentUnitRepository.GetByFulfilmentIdAsync(locationId, cancellationToken);
        var market = fulfilmentUnit?.CountryCode ?? UnknownMarket;

        var request = new DeltaTowardsOmsPublishRequest
        {
            ReferenceId = Guid.NewGuid().ToString(),
            ProductId = productId,
            Location = new PublishLocation(locationId, locationType),
            Reason = "ADJUSTMENT",
            AdjustmentDate = timeProvider.GetUtcNow().UtcDateTime,
            ProductUnits = "N/A",
            Market = market,
            QuantityDetails =
            [
                new DeltaTowardsOmsQuantityDetail
                {
                    CountryOfOrigin = countryOfOrigin,
                    Hallmarking = hallmarking,
                    Quantity = deltaTowardsOms,
                    State = State.AVAILABLE.ToString(),
                    Status = Status.PICKABLE.ToString(),
                    ReasonTexts = [],
                },
            ],
        };

        var json = JsonSerializer.Serialize(request);

        var relayMessage = new ServiceBusRelayMessage(
            QueueName: publishOptions.Value.OmsDeltaQueueName,
            SessionId: request.ReferenceId,
            MessageId: $"{request.ReferenceId}:{EventTypeName}",
            CorrelationId: correlationContext.CorrelationId,
            AppId: correlationContext.AppId,
            Types: [EventTypeName],
            SourceName: nameof(DeltaTowardsOmsPublisher),
            PayloadName: EventTypeName,
            Json: json);

        await relayPublisher.PublishAsync(relayMessage, cancellationToken);

        logger.LogInformation(
            "Published §3.7 OMS delta event for ProductId {ProductId}, Delta {Delta}, Market {Market} to queue {QueueName}.",
            productId, deltaTowardsOms, market, relayMessage.QueueName);
    }
}
