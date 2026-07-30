using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <inheritdoc cref="IItemStockInventoryExtendedSegmentationService"/>
public sealed class ItemStockInventoryExtendedSegmentationService(
    IItemStockInventoryExtendedRepository repository,
    ICorrelationContext correlationContext,
    ILogger<ItemStockInventoryExtendedSegmentationService> logger) : IItemStockInventoryExtendedSegmentationService
{
    /// <inheritdoc/>
    public async Task ApplyAsync(
        string fulfilmentId, string itemCode, string hallmark, string countryOfOrigin,
        State fromState, Status fromStatus, State toState, Status toStatus,
        int? quantity, CancellationToken cancellationToken = default)
    {
        // Mirrors Reflex's ExtendedInventorySegmentationEventHandler.HandleAsync exactly - the two
        // gates are deliberately asymmetric, not a symmetric "not baseline Available/Pickable" check
        // on both sides. isValidToState starts true and is only negated for the baseline pair;
        // isValidFromState additionally starts false outright for a B2B_INVENTORY_ADJUSTED event
        // (Fix SAE-3032's correlation-context-type suppression).
        var isValidToState = !(toState == State.AVAILABLE && toStatus == Status.PICKABLE);
        var isValidFromState = correlationContext.Type != KafkaEvents.InventoryAdjustedEventType
            && !(fromState == State.AVAILABLE && fromStatus == Status.PICKABLE);

        if (isValidToState)
        {
            await ApplyToStateAsync(fulfilmentId, itemCode, hallmark, countryOfOrigin, toState, toStatus, quantity, cancellationToken);
        }

        if (isValidFromState)
        {
            await ApplyFromStateAsync(fulfilmentId, itemCode, hallmark, countryOfOrigin, fromState, fromStatus, quantity, cancellationToken);
        }
    }

    /// <summary>Fetches or creates the to-state extended record and increments its quantity by the inbound amount.</summary>
    private async Task ApplyToStateAsync(
        string fulfilmentId, string itemCode, string hallmark, string countryOfOrigin,
        State toState, Status toStatus, int? quantity, CancellationToken cancellationToken)
    {
        var existing = await repository.GetAsync(fulfilmentId, itemCode, hallmark, countryOfOrigin, toState, toStatus, cancellationToken);

        if (existing is null)
        {
            var created = new ItemStockInventoryExtended
            {
                FulfilmentId = fulfilmentId,
                ItemCode = itemCode,
                Hallmark = hallmark,
                COO = countryOfOrigin,
                State = toState,
                Status = toStatus,
                Qty = quantity,
            };

            await repository.CreateAsync(created, cancellationToken);

            logger.LogInformation(
                "Created §3.5 extended inventory record {Id} with Qty={Qty}.", created.Id, created.Qty);

            return;
        }

        existing.Qty = (existing.Qty ?? 0) + quantity;
        await repository.ReplaceAsync(existing, existing.ETag!, cancellationToken);

        logger.LogInformation(
            "Incremented §3.5 extended inventory record {Id} to Qty={Qty}.", existing.Id, existing.Qty);
    }

    /// <summary>Decrements the from-state extended record if it holds enough quantity, otherwise logs a warning and skips (never throws on oversell).</summary>
    private async Task ApplyFromStateAsync(
        string fulfilmentId, string itemCode, string hallmark, string countryOfOrigin,
        State fromState, Status fromStatus, int? quantity, CancellationToken cancellationToken)
    {
        var existing = await repository.GetAsync(fulfilmentId, itemCode, hallmark, countryOfOrigin, fromState, fromStatus, cancellationToken);

        if (quantity is not null && existing?.Qty >= Math.Abs(quantity.Value))
        {
            existing.Qty -= quantity;
            await repository.ReplaceAsync(existing, existing.ETag!, cancellationToken);

            logger.LogInformation(
                "Decremented §3.5 extended inventory record {Id} to Qty={Qty}.", existing.Id, existing.Qty);

            return;
        }

        logger.LogWarning(
            "FromState {FromState} and FromStatus {FromStatus} value for item {ItemCode} cannot be negative - " +
            "skipping §3.5 from-state decrement (FulfilmentId={FulfilmentId}, Hallmark={Hallmark}, COO={Coo}, Quantity={Quantity}).",
            fromState, fromStatus, itemCode, fulfilmentId, hallmark, countryOfOrigin, quantity);
    }
}
