using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Egress;

/// <inheritdoc cref="IStockOnHandUpdatedOmsPublisher"/>
internal sealed class StockOnHandUpdatedOmsPublisher(
    IFulfilmentUnitRepository fulfilmentUnitRepository,
    ICountryRepository countryRepository,
    IServiceBusRelayPublisher relayPublisher,
    IOptions<InventoryPublishOptions> publishOptions,
    ICorrelationContext correlationContext,
    ILogger<StockOnHandUpdatedOmsPublisher> logger) : IStockOnHandUpdatedOmsPublisher
{
    private const string EventTypeName = "Inventory_B2CStockOnHandUpdated";
    private const string UnknownMarket = "UNKNOWN";
    private const string FixedChannel = "OWN_ONLINE";

    /// <inheritdoc/>
    public async Task PublishAsync(
        string fulfilmentId,
        string locationType,
        string productId,
        string productUnits,
        string? entity,
        string? barcode,
        IReadOnlyList<StockOnHandUpdatedOmsQuantityDetail> quantityDetails,
        string reason,
        DateTime updatedDate,
        string eventId,
        CancellationToken cancellationToken = default)
    {
        var fulfilmentUnit = await fulfilmentUnitRepository.GetByFulfilmentIdAsync(fulfilmentId, cancellationToken);
        var market = fulfilmentUnit?.CountryCode ?? UnknownMarket;

        if (market != UnknownMarket)
        {
            var countryMaster = await countryRepository.GetByCodeAsync(market, cancellationToken);
            if (countryMaster is null || !countryMaster.IsActive)
            {
                logger.LogWarning(
                    "Market {Market} resolved from FulfilmentUnit {FulfilmentId} has no active CountryMaster record.",
                    market, fulfilmentId);
            }
        }

        var request = new StockOnHandUpdatedOmsPublishRequest
        {
            ProductId = productId,
            ProductUnits = productUnits,
            Location = new PublishLocation(fulfilmentId, locationType),
            Entity = entity,
            Barcode = barcode,
            Market = market,
            Reason = reason,
            UpdatedDate = updatedDate,
            Channel = FixedChannel,
            QuantityDetails = quantityDetails,
        };

        var json = JsonSerializer.Serialize(request);
        var referenceId = $"{fulfilmentId}:{productId}:{eventId}";

        var relayMessage = new ServiceBusRelayMessage(
            QueueName: publishOptions.Value.IcrSnapshotQueueName,
            SessionId: referenceId,
            MessageId: $"{referenceId}:{EventTypeName}",
            CorrelationId: correlationContext.CorrelationId,
            AppId: correlationContext.AppId,
            Types: [EventTypeName],
            SourceName: nameof(StockOnHandUpdatedOmsPublisher),
            PayloadName: EventTypeName,
            Json: json);

        await relayPublisher.PublishAsync(relayMessage, cancellationToken);

        logger.LogInformation(
            "Published §7.3 B2C stock notification for ProductId {ProductId}, Market {Market} to queue {QueueName}.",
            productId, market, relayMessage.QueueName);
    }
}
