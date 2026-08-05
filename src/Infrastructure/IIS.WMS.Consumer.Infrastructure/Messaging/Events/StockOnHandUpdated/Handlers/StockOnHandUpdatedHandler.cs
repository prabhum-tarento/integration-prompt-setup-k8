using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using DomainEnums = IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockOnHandUpdated.Handlers;

/// <summary>
/// Applies one relayed <see cref="StockOnHandUpdatedEvent"/> (docs/events/inventory.StockOnHandUpdated.md).
/// B2C-only, BRZ3PL-only (Business Rule 1) - unlike <see cref="StockSyncSubmitted.Handlers.StockSyncSubmittedHandler"/>,
/// this doc's own Sellable/Non-Sellable sets (Business Rule 4) are mutually exclusive, so they are defined
/// fresh here rather than reused. The non-sellable path is a discrepancy-gated <c>PatchOperation.Increment</c>
/// by delta (Calculation 2) - not the unconditional-increment precedent in
/// <see cref="GoodsInTransitReceivedService"/> and not the absolute <c>Set</c> precedent in
/// <see cref="StockSyncSubmittedHandler"/>'s extended-state path. The B2C notification (§7.3) publishes
/// unconditionally per group (no feature-flag/availability gate); only the ICR snapshot is gated by
/// <see cref="FeatureFlagsOptions.EnableSnapshotForIcr"/>.
/// </summary>
/// <param name="itemStockInventoryService">§5.1 sellable stock Set (reused as-is from the StockSyncSubmitted pipeline - an exact match for this doc's Calculation 1).</param>
/// <param name="extendedRepository">§5.2 non-sellable/extended-state tracking, hand-rolled here for the discrepancy-gated Increment semantics.</param>
/// <param name="itemRepository">Item master existence check + auto-create (assumption 5).</param>
/// <param name="omsPublisher">§7.3 B2C stock notification publisher.</param>
/// <param name="inventoryComparisonReportPublisher">§9 optional ICR snapshot, gated by <see cref="FeatureFlagsOptions.EnableSnapshotForIcr"/>.</param>
/// <param name="archiveWriter">§5.3 before/after archival (best-effort, non-blocking).</param>
/// <param name="featureFlagsOptions">Gates the ICR snapshot publish.</param>
/// <param name="consumerOptions">Carries <see cref="StockOnHandUpdatedServiceBusConsumerOptions.MaxItemLineParallelism"/>, the bounded fan-out for per-group processing.</param>
/// <param name="timeProvider">Injectable clock for timestamps (archive entries and Cosmos <c>Timestamp</c> fields).</param>
/// <param name="logger">Logger for stock-on-hand processing.</param>
public sealed class StockOnHandUpdatedHandler(
    IItemStockInventoryService itemStockInventoryService,
    IItemStockInventoryExtendedRepository extendedRepository,
    IItemRepository itemRepository,
    IStockOnHandUpdatedOmsPublisher omsPublisher,
    IInventoryComparisonReportPublisher inventoryComparisonReportPublisher,
    IMessageArchiveWriter archiveWriter,
    IOptions<FeatureFlagsOptions> featureFlagsOptions,
    IOptions<StockOnHandUpdatedServiceBusConsumerOptions> consumerOptions,
    TimeProvider timeProvider,
    ILogger<StockOnHandUpdatedHandler> logger)
    : IStockOnHandUpdatedHandler
{
    private const int MaxConcurrencyRetryAttempts = 3;

    private static readonly HashSet<(DomainEnums.State State, DomainEnums.Status Status)> SellableStates =
    [
        (DomainEnums.State.AVAILABLE, DomainEnums.Status.PREPARED),
        (DomainEnums.State.AVAILABLE, DomainEnums.Status.PICKABLE),
        (DomainEnums.State.AVAILABLETOSELL, DomainEnums.Status.PICKABLE),
    ];

    private static readonly HashSet<(DomainEnums.State State, DomainEnums.Status Status)> NonSellableStates =
    [
        (DomainEnums.State.AVAILABLE, DomainEnums.Status.HELD),
        (DomainEnums.State.INSPECTION, DomainEnums.Status.PICKABLE),
    ];

    /// <inheritdoc/>
    public async Task HandleAsync(StockOnHandUpdatedEvent message, string correlationId, CancellationToken cancellationToken)
    {
        if (message.Location.Id != FulfilmentLocationIds.Brz3PlConsigneeId)
        {
            logger.LogInformation(
                "STOCK_ON_HAND_UPDATED_IGNORED: Location {LocationId} is not {ExpectedLocationId} - skipping (Business Rule 1).",
                message.Location.Id, FulfilmentLocationIds.Brz3PlConsigneeId);
            return;
        }

        const string fulfilmentId = FulfilmentLocationIds.BrzDc3PlFulfilmentId;

        await EnsureItemExistsAsync(message.ProductId, cancellationToken);

        var groups = message.QuantityDetails
            .Where(detail => detail.Domain == StockOnHandInventoryDomain.B2C)
            .Where(detail => IsRelevant(detail.State))
            .GroupBy(detail => (detail.CountryOfOrigin, detail.Hallmarking))
            .ToList();

        if (groups.Count == 0)
        {
            return;
        }

        await ForEachGroupAsync(
            groups,
            group => ProcessGroupAsync(message, fulfilmentId, group.Key.CountryOfOrigin, group.Key.Hallmarking, group.ToList(), correlationId, cancellationToken),
            cancellationToken);
    }

    private static bool IsRelevant(InventoryEventStateSnapshot state)
    {
        var pair = ToPair(state);
        return SellableStates.Contains(pair) || NonSellableStates.Contains(pair);
    }

    /// <summary>Assumption 5 - auto-creates a missing item master record, warning rather than failing.</summary>
    private async Task EnsureItemExistsAsync(string itemCode, CancellationToken cancellationToken)
    {
        var existing = await itemRepository.GetByItemCodeAsync(itemCode, cancellationToken);
        if (existing is not null)
        {
            return;
        }

        logger.LogWarning(
            "Item master record {ItemCode} not found - auto-creating (docs/events/inventory.StockOnHandUpdated.md assumption 5).",
            itemCode);
        await itemRepository.CreateAsync(new Item { ItemCode = itemCode }, cancellationToken);
    }

    /// <summary>
    /// §4 per-group processing: sellable (§5.1) and non-sellable (§5.2) are isolated from each other
    /// (§8 "a failure in the sellable path... does not stop the non-sellable path, and vice versa") -
    /// each runs in its own try/catch and any non-cancellation failure is captured rather than stopping
    /// the other. The B2C notification (§7.3) and optional ICR snapshot (§9) always follow, matching the
    /// mermaid state machine's unconditional convergence into the publish step. Captured failures are
    /// rethrown afterwards (prioritizing <see cref="ConcurrencyException"/>) so the outer per-message
    /// outcome mapping (§8: Abandoned/DeadLettered) still applies.
    /// </summary>
    private async Task ProcessGroupAsync(
        StockOnHandUpdatedEvent message, string fulfilmentId, string countryOfOrigin, string hallmarking,
        IReadOnlyList<StockOnHandQuantityDetail> details, string correlationId, CancellationToken cancellationToken)
    {
        var exceptions = new List<Exception>();

        try
        {
            await ProcessSellableAsync(message, fulfilmentId, countryOfOrigin, hallmarking, details, correlationId, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            exceptions.Add(ex);
            logger.LogWarning(
                ex,
                "§5.1 sellable processing failed for {ItemCode}/{FulfilmentId}/{CountryOfOrigin}/{Hallmarking} - continuing with non-sellable.",
                message.ProductId, fulfilmentId, countryOfOrigin, hallmarking);
        }

        try
        {
            await ProcessNonSellableAsync(fulfilmentId, message.ProductId, countryOfOrigin, hallmarking, details, correlationId, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            exceptions.Add(ex);
            logger.LogWarning(
                ex,
                "§5.2 non-sellable processing failed for {ItemCode}/{FulfilmentId}/{CountryOfOrigin}/{Hallmarking} - continuing.",
                message.ProductId, fulfilmentId, countryOfOrigin, hallmarking);
        }

        await PublishB2CNotificationAsync(message, fulfilmentId, countryOfOrigin, hallmarking, details, cancellationToken);

        if (featureFlagsOptions.Value.EnableSnapshotForIcr)
        {
            var isThirdPartyLogistics = message.Location.Id == FulfilmentLocationIds.Caecom;
            await inventoryComparisonReportPublisher.PublishAsync(
                fulfilmentId, message.ProductId, hallmarking, countryOfOrigin, isThirdPartyLogistics, cancellationToken);
        }

        RethrowIfAny(exceptions);
    }

    /// <summary>Business Rule 4 sellable set + Business Rule 6/Calculation 1 - Set (never Increment) via the reused stock-sync service.</summary>
    private async Task ProcessSellableAsync(
        StockOnHandUpdatedEvent message, string fulfilmentId, string countryOfOrigin, string hallmarking,
        IReadOnlyList<StockOnHandQuantityDetail> details, string correlationId, CancellationToken cancellationToken)
    {
        var sellableDetails = details.Where(detail => SellableStates.Contains(ToPair(detail.State))).ToList();
        if (sellableDetails.Count == 0)
        {
            return;
        }

        var b2cPrepared = QuantityFor(sellableDetails, DomainEnums.State.AVAILABLE, DomainEnums.Status.PREPARED);
        var b2cAvailableToSell = QuantityFor(sellableDetails, DomainEnums.State.AVAILABLETOSELL, DomainEnums.Status.PICKABLE);
        var b2cAvl = b2cAvailableToSell + b2cPrepared;

        var beforeJson = JsonSerializer.Serialize(new { fulfilmentId, message.ProductId, countryOfOrigin, hallmarking });
        ArchiveState("stock-on-hand-sellable-before", message.ProductId, beforeJson, correlationId);

        await itemStockInventoryService.ApplyStockSyncAsync(
            fulfilmentId, message.ProductId, countryOfOrigin, hallmarking, b2cAvl, b2cPrepared, b2cAvailableToSell, cancellationToken);

        var afterJson = JsonSerializer.Serialize(new { fulfilmentId, message.ProductId, countryOfOrigin, hallmarking, b2cAvl, b2cPrepared, b2cAvailableToSell });
        ArchiveState("stock-on-hand-sellable-after", message.ProductId, afterJson, correlationId);
    }

    /// <summary>Business Rule 4 non-sellable set - one discrepancy-gated Increment-by-delta call per line (Calculation 2).</summary>
    private async Task ProcessNonSellableAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmarking,
        IReadOnlyList<StockOnHandQuantityDetail> details, string correlationId, CancellationToken cancellationToken)
    {
        var nonSellableDetails = details.Where(detail => NonSellableStates.Contains(ToPair(detail.State))).ToList();

        foreach (var detail in nonSellableDetails)
        {
            var (state, status) = ToPair(detail.State);
            var quantity = Normalize(detail.Quantity);

            await ApplyExtendedQuantityAsync(fulfilmentId, itemCode, countryOfOrigin, hallmarking, state, status, quantity, correlationId, cancellationToken);
        }
    }

    /// <summary>
    /// §5.2/Calculation 2 - fetch/create-if-missing, then a Patch <c>Increment("/Qty", delta)</c> only
    /// when <c>delta != 0</c> (a discrepancy); otherwise a pure no-op, unlike
    /// <see cref="GoodsInTransitReceivedService"/>'s unconditional-increment precedent. ETag-conflict
    /// retry loop copies that same precedent's shape.
    /// </summary>
    private async Task ApplyExtendedQuantityAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        DomainEnums.State state, DomainEnums.Status status, int quantity, string correlationId, CancellationToken cancellationToken)
    {
        var id = ItemStockInventoryExtended.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin, state, status);

        for (var attempt = 1; attempt <= MaxConcurrencyRetryAttempts; attempt++)
        {
            var existing = await extendedRepository.GetAsync(fulfilmentId, itemCode, hallmark, countryOfOrigin, state, status, cancellationToken);
            var previousQty = existing?.Qty ?? 0;
            var delta = quantity - previousQty;
            var wasCreated = existing is null;

            if (!wasCreated && delta == 0)
            {
                return;
            }

            var beforeJson = JsonSerializer.Serialize(new { fulfilmentId, itemCode, countryOfOrigin, hallmark, state, status, previousQty });
            ArchiveState("stock-on-hand-extended-before", itemCode, beforeJson, correlationId);

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
                        id, id, existing!.ETag!,
                        [
                            PatchOperation.Increment("/Qty", delta),
                            PatchOperation.Set("/Timestamp", nowUtc),
                        ],
                        cancellationToken);
                }

                var afterJson = JsonSerializer.Serialize(new { fulfilmentId, itemCode, countryOfOrigin, hallmark, state, status, quantity, delta });
                ArchiveState("stock-on-hand-extended-after", itemCode, afterJson, correlationId);

                logger.LogInformation(
                    "STOCK_ON_HAND_EXTENDED_APPLIED: {Id} (item {ItemCode}) delta={Delta}, created={WasCreated}.",
                    id, itemCode, delta, wasCreated);

                return;
            }
            catch (ConcurrencyException ex) when (attempt < MaxConcurrencyRetryAttempts && !wasCreated)
            {
                logger.LogWarning(
                    ex,
                    "CONCURRENCY_CONFLICT: stock-on-hand extended update for {Id} (item {ItemCode}) failed on attempt {Attempt}, retrying.",
                    id, itemCode, attempt);
            }
        }

        throw new ConcurrencyException(id, "Exhausted retry attempts for stock-on-hand extended update");
    }

    /// <summary>§7.3 - unconditional per-group publish (no feature-flag/availability gate), after the Cosmos writes above.</summary>
    private async Task PublishB2CNotificationAsync(
        StockOnHandUpdatedEvent message, string fulfilmentId, string countryOfOrigin, string hallmarking,
        IReadOnlyList<StockOnHandQuantityDetail> details, CancellationToken cancellationToken)
    {
        var quantityDetails = details.Select(detail =>
        {
            var (state, status) = ToPair(detail.State);
            return new StockOnHandUpdatedOmsQuantityDetail
            {
                Quantity = Normalize(detail.Quantity),
                State = state.ToString(),
                Status = status.ToString(),
                CountryOfOrigin = detail.CountryOfOrigin,
                Hallmarking = detail.Hallmarking,
            };
        }).ToList();

        var eventId = $"{message.ReferenceId}:{countryOfOrigin}:{hallmarking}";

        await omsPublisher.PublishAsync(
            fulfilmentId,
            message.Location.Type.ToString(),
            message.ProductId,
            message.ProductUnits,
            message.Entity,
            message.Barcode,
            quantityDetails,
            message.Reason.ToString(),
            message.UpdatedDate,
            eventId,
            cancellationToken);
    }

    private void ArchiveState(string category, string itemCode, string payload, string correlationId) =>
        archiveWriter.Enqueue(MessageArchive.Create($"{category}:{itemCode}:{correlationId}", category, payload, correlationId, timeProvider.GetUtcNow().UtcDateTime));

    private static (DomainEnums.State State, DomainEnums.Status Status) ToPair(InventoryEventStateSnapshot state) =>
        (InventoryEventStateMapper.ToDomainState(state.State), InventoryEventStateMapper.ToDomainStatus(state.Status));

    /// <summary>Business Rule 6 - <c>FirstOrDefault</c> per (State, Status) pair, normalized, defaulting to 0.</summary>
    private static int QuantityFor(IReadOnlyList<StockOnHandQuantityDetail> details, DomainEnums.State state, DomainEnums.Status status) =>
        Normalize(details.Where(detail => ToPair(detail.State) == (state, status)).Select(detail => detail.Quantity).FirstOrDefault());

    /// <summary>Business Rule 5 - negative quantities are normalized to 0, never negative in final state.</summary>
    private static int Normalize(int quantity) => Math.Max(0, quantity);

    private static void RethrowIfAny(List<Exception> exceptions)
    {
        if (exceptions.Count == 0)
        {
            return;
        }

        if (exceptions.FirstOrDefault(ex => ex is ConcurrencyException) is { } concurrencyException)
        {
            ExceptionDispatchInfo.Capture(concurrencyException).Throw();
        }

        if (exceptions.Count == 1)
        {
            ExceptionDispatchInfo.Capture(exceptions[0]).Throw();
        }

        throw new AggregateException(exceptions);
    }

    /// <summary>
    /// Bounded-parallel fan-out (integration-resiliency.instructions.md §6) over one message's
    /// (CountryOfOrigin, Hallmarking) groups - copies <see cref="StockSyncSubmittedHandler.ForEachGroupAsync{T}"/>'s
    /// exception-collection/prioritization exactly.
    /// </summary>
    private async Task ForEachGroupAsync<T>(
        IReadOnlyCollection<T> groups, Func<T, Task> processGroupAsync, CancellationToken cancellationToken)
    {
        var exceptions = new ConcurrentQueue<Exception>();

        await Parallel.ForEachAsync(
            groups,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = consumerOptions.Value.MaxItemLineParallelism,
                CancellationToken = cancellationToken,
            },
            async (group, _) =>
            {
                try
                {
                    await processGroupAsync(group);
                }
                catch (Exception ex)
                {
                    exceptions.Enqueue(ex);
                }
            });

        if (exceptions.IsEmpty)
        {
            return;
        }

        if (exceptions.FirstOrDefault(ex => ex is ConcurrencyException) is { } concurrencyException)
        {
            ExceptionDispatchInfo.Capture(concurrencyException).Throw();
        }

        if (exceptions.FirstOrDefault(ex => ex is OperationCanceledException) is { } canceledException)
        {
            ExceptionDispatchInfo.Capture(canceledException).Throw();
        }

        var others = exceptions.ToArray();
        if (others.Length == 1)
        {
            ExceptionDispatchInfo.Capture(others[0]).Throw();
        }

        throw new AggregateException(others);
    }
}
