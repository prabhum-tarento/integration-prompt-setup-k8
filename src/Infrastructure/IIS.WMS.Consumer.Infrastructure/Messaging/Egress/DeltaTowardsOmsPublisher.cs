using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Egress;

/// <inheritdoc cref="IDeltaTowardsOmsPublisher"/>
internal sealed class DeltaTowardsOmsPublisher(
    IFulfilmentUnitRepository fulfilmentUnitRepository,
    ICountryRepository countryRepository,
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
        string eventId,
        CancellationToken cancellationToken = default)
    {
        var fulfilmentUnit = await fulfilmentUnitRepository.GetByFulfilmentIdAsync(locationId, cancellationToken);
        var market = fulfilmentUnit?.CountryCode ?? UnknownMarket;

        if (market != UnknownMarket)
        {
            var countryMaster = await countryRepository.GetByCodeAsync(market, cancellationToken);
            if (countryMaster is null || !countryMaster.IsActive)
            {
                logger.LogWarning(
                    "Market {Market} resolved from FulfilmentUnit {LocationId} has no active CountryMaster record.",
                    market, locationId);
            }
        }

        var request = new DeltaTowardsOmsPublishRequest
        {
            ReferenceId = $"{locationId}:{productId}:{eventId}",
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
