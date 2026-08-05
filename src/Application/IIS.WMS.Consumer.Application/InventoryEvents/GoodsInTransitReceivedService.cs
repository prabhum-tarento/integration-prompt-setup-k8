using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <inheritdoc cref="IGoodsInTransitReceivedService"/>
public sealed class GoodsInTransitReceivedService(
    IItemStockInventoryRepository repository,
    IItemStockInventoryExtendedRepository extendedRepository,
    TimeProvider timeProvider,
    ILogger<GoodsInTransitReceivedService> logger) : IGoodsInTransitReceivedService
{
    private const int MaxConcurrencyRetryAttempts = 3;

    public Task<GoodsInTransitReceiptResult> ReceiveShipmentLineAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark, int quantity,
        bool isSellable, State state, Status status, CancellationToken cancellationToken = default) =>
        isSellable
            ? ReceiveSellableAsync(fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity, cancellationToken)
            : ReceiveNonSellableAsync(fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity, state, status, cancellationToken);

    /// <summary>§3.2/§6.1 sellable receipt - accumulates onto the main <see cref="ItemStockInventory"/> record's B2C available bucket.</summary>
    private async Task<GoodsInTransitReceiptResult> ReceiveSellableAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark, int quantity,
        CancellationToken cancellationToken)
    {
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);
        var result = new GoodsInTransitReceiptResult();

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var aggregate = await repository.GetAsync(id, id, cancellationToken);
            var wasCreated = aggregate is null;
            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

            if (wasCreated)
            {
                aggregate = ItemStockInventory.CreateDefault(fulfilmentId, itemCode, hallmark, countryOfOrigin, nowUtc);
                aggregate.UpdateB2CAvailable(quantity);
            }

            result.IsB2CChanged = true;
            result.DeltaTowardsOms = quantity;

            try
            {
                if (wasCreated)
                {
                    await repository.CreateAsync(aggregate!, cancellationToken);
                }
                else
                {
                    await repository.PatchAsync(
                        id, id, aggregate!.ETag!,
                        [
                            PatchOperation.Increment("/B2CAVL", quantity),
                            PatchOperation.Set("/Timestamp", nowUtc.ToString("O")),
                        ],
                        cancellationToken);
                }

                logger.LogInformation(
                    "GOODS_IN_TRANSIT_RECEIVED_SELLABLE_APPLIED: {Id} (item {ItemCode}) received quantity={Quantity}, created={WasCreated}.",
                    id, itemCode, quantity, wasCreated);

                return result;
            }
            catch (ConcurrencyException ex) when (attempt < MaxConcurrencyRetryAttempts && !wasCreated)
            {
                logger.LogWarning(
                    ex,
                    "CONCURRENCY_CONFLICT: goods-in-transit sellable receipt for {Id} (item {ItemCode}) failed on attempt {Attempt}, retrying.",
                    id, itemCode, attempt);
            }
        }

        throw new ConcurrencyException(id, "Exhausted retry attempts for goods-in-transit sellable receipt");
    }

    /// <summary>
    /// §3.7/§6.2 non-sellable receipt - accumulates onto the (State, Status)-keyed
    /// <see cref="ItemStockInventoryExtended"/> record via <c>PatchOperation.Increment</c>, never
    /// <c>ReplaceAsync</c> (the last-write-wins anti-pattern the doc explicitly warns against - see
    /// <see cref="ItemStockInventoryExtendedSegmentationService"/> for the pattern this must NOT repeat).
    /// Also ensures a zeroed main <see cref="ItemStockInventory"/> record exists for the same key
    /// (create-if-missing only, never incremented here) so downstream sellable-path reads never hit a
    /// missing record for an item that has only ever received non-sellable stock.
    /// </summary>
    private async Task<GoodsInTransitReceiptResult> ReceiveNonSellableAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark, int quantity,
        State state, Status status, CancellationToken cancellationToken)
    {
        var result = new GoodsInTransitReceiptResult();

        await EnsureMainRecordExistsAsync(fulfilmentId, itemCode, countryOfOrigin, hallmark, cancellationToken);

        var id = ItemStockInventoryExtended.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin, state, status);

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var entity = await extendedRepository.GetAsync(
                fulfilmentId, itemCode, hallmark, countryOfOrigin, state, status, cancellationToken);
            var wasCreated = entity is null;
            var nowUtc = timeProvider.GetUtcNow().UtcDateTime;

            try
            {
                if (wasCreated)
                {
                    await extendedRepository.CreateAsync(
                        new ItemStockInventoryExtended
                        {
                            FulfilmentId = fulfilmentId,
                            ItemCode = itemCode,
                            COO = countryOfOrigin,
                            Hallmark = hallmark,
                            State = state,
                            Status = status,
                            Qty = quantity,
                            SubmittedDate = nowUtc,
                        },
                        cancellationToken);
                }
                else
                {
                    await extendedRepository.PatchAsync(
                        id, id, entity!.ETag!,
                        [
                            PatchOperation.Increment("/Qty", quantity),
                            PatchOperation.Set("/Timestamp", nowUtc),
                        ],
                        cancellationToken);
                }

                logger.LogInformation(
                    "GOODS_IN_TRANSIT_RECEIVED_NON_SELLABLE_APPLIED: {Id} (item {ItemCode}) received quantity={Quantity}, created={WasCreated}.",
                    id, itemCode, quantity, wasCreated);

                return result;
            }
            catch (ConcurrencyException ex) when (attempt < MaxConcurrencyRetryAttempts && !wasCreated)
            {
                logger.LogWarning(
                    ex,
                    "CONCURRENCY_CONFLICT: goods-in-transit non-sellable receipt for {Id} (item {ItemCode}) failed on attempt {Attempt}, retrying.",
                    id, itemCode, attempt);
            }
        }

        throw new ConcurrencyException(id, "Exhausted retry attempts for goods-in-transit non-sellable receipt");
    }

    /// <summary>
    /// §3.7/§6.2 "ensure main record exists with zeros" - create-if-missing only, never increments an
    /// existing record. Concurrency conflicts on the create race are ignored: <see cref="IItemStockInventoryRepository.CreateAsync"/>
    /// is redelivery-safe (re-reads and returns the existing item on a Cosmos conflict), so a concurrent
    /// creator racing this call is not an error here.
    /// </summary>
    private async Task EnsureMainRecordExistsAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark, CancellationToken cancellationToken)
    {
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);
        var existing = await repository.GetAsync(id, id, cancellationToken);

        if (existing is not null)
        {
            return;
        }

        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        var zeroed = ItemStockInventory.CreateDefault(fulfilmentId, itemCode, hallmark, countryOfOrigin, nowUtc);

        await repository.CreateAsync(zeroed, cancellationToken);
    }
}
