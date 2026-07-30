using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

/// <inheritdoc cref="IInventoryComparisonReportPublisher"/>
internal sealed class InventoryComparisonReportPublisher(
    IItemStockInventoryRepository repository,
    IServiceBusRelayPublisher relayPublisher,
    IOptions<InventoryPublishOptions> publishOptions,
    ICorrelationContext correlationContext,
    TimeProvider timeProvider,
    ILogger<InventoryComparisonReportPublisher> logger) : IInventoryComparisonReportPublisher
{
    private const string EventTypeName = "Inventory_OmniInventoryAvailabilityReported";
    private const string B2BDomain = "B2B";
    private const string B2CDomain = "B2C";

    /// <inheritdoc/>
    public async Task PublishAsync(
        string fulfilmentId, string itemCode, string hallmark, string countryOfOrigin,
        bool isThirdPartyLogistics, CancellationToken cancellationToken = default)
    {
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);
        var inventory = await repository.GetAsync(id, id, cancellationToken);

        if (inventory is null)
        {
            logger.LogInformation(
                "Skipping §3.8 ICR snapshot for Id {Id} - no ItemStockInventory record found.", id);

            return;
        }

        var b2cAvailableQuantity = inventory.IsExtended ? inventory.B2COriginal : inventory.B2CAvailable;

        var request = new OmniInventoryAvailabilityPublishRequest
        {
            ReportDate = timeProvider.GetUtcNow().UtcDateTime,
            Location = new PublishLocation(
                fulfilmentId,
                isThirdPartyLogistics ? "ThirdPartyLogistics" : "Warehouse"),
            ProductId = itemCode,
            ProductUnits = "N/A",
            CountryOfOrigin = countryOfOrigin,
            Hallmarking = hallmark,
            QuantityDetails =
            [
                new OmniInventoryQuantityDetail
                {
                    Quantity = inventory.B2BAvailable,
                    State = State.AVAILABLE.ToString(),
                    Status = Status.PICKABLE.ToString(),
                    Domain = B2BDomain,
                },
                new OmniInventoryQuantityDetail
                {
                    Quantity = b2cAvailableQuantity,
                    State = State.AVAILABLE.ToString(),
                    Status = Status.PICKABLE.ToString(),
                    Domain = B2CDomain,
                },
                new OmniInventoryQuantityDetail
                {
                    Quantity = inventory.B2BPrepared,
                    State = State.AVAILABLE.ToString(),
                    Status = Status.PREPARED.ToString(),
                    Domain = B2BDomain,
                },
                new OmniInventoryQuantityDetail
                {
                    Quantity = inventory.B2CPrepared,
                    State = State.AVAILABLE.ToString(),
                    Status = Status.PREPARED.ToString(),
                    Domain = B2CDomain,
                },
            ],
        };

        var json = JsonSerializer.Serialize(request);

        var relayMessage = new ServiceBusRelayMessage(
            QueueName: publishOptions.Value.IcrSnapshotQueueName,
            SessionId: id,
            MessageId: $"{id}:{EventTypeName}",
            CorrelationId: correlationContext.CorrelationId,
            AppId: correlationContext.AppId,
            Types: [EventTypeName],
            SourceName: nameof(InventoryComparisonReportPublisher),
            PayloadName: EventTypeName,
            Json: json);

        await relayPublisher.PublishAsync(relayMessage, cancellationToken);

        logger.LogInformation("Published §3.8 ICR snapshot for Id {Id} to queue {QueueName}.", id, relayMessage.QueueName);
    }
}
