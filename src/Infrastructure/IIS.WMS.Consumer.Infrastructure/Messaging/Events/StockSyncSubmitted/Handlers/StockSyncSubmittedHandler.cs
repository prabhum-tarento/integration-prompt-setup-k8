using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockSyncSubmitted;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using DomainEnums = IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockSyncSubmitted.Handlers;

/// <summary>
/// Applies one relayed <see cref="StockSyncSubmittedEvent"/> (docs/events/inventory.StockSyncSubmitted.md).
/// Unlike <see cref="InventoryAdjusted.Handlers.InventoryAdjustedHandler"/>, this event's item lines are
/// grouped by (CountryOfOrigin, Hallmarking) per §3.1 rather than processed individually - each group
/// carries its own sellable (§3.2/§4.2) and non-sellable/extended (§3.3/§4b) quantity extraction. The
/// extended-state call reuses <see cref="IItemStockInventoryExtendedSegmentationService.ApplyAsync"/>
/// with the same (State, Status) for both from/to, same precedent as <c>InventoryAdjustedHandler</c>,
/// since this event also carries single state/status snapshots rather than a From/To pair. Snapshot
/// save (§3.4) always runs per processed state/status, regardless of whether the sellable/discrepancy
/// path changed anything. The OMS B2C snapshot (§3.5) publishes once per group, after the Cosmos write
/// commits, gated by <see cref="FeatureFlagsOptions.EnableSnapshotTowardsOmsBrz3Pl"/> + the §3.5
/// availability gate.
/// </summary>
/// <param name="itemStockInventoryService">§3.2 sellable stock-sync Set + discrepancy detection input (<c>ItemStockSyncApplyResult.PreviousB2CAvailable</c>).</param>
/// <param name="extendedSegmentationService">§3.3/§4b non-sellable/extended-state tracking.</param>
/// <param name="itemDiscrepencyDetailRepository">§5.4 discrepancy audit persistence.</param>
/// <param name="snapshotStockSyncItemRepository">§5.3/§3.4 inventory-sync snapshot persistence.</param>
/// <param name="itemRepository">Item master existence check + auto-create (assumption 3).</param>
/// <param name="omsPublisher">§3.5 OMS B2C stock snapshot publisher.</param>
/// <param name="archiveWriter">§5.5 before/after archival (best-effort, non-blocking).</param>
/// <param name="featureFlagsOptions">Gates the §3.5 OMS snapshot publish for the BRZ3PL location.</param>
/// <param name="consumerOptions">Carries <see cref="StockSyncSubmittedServiceBusConsumerOptions.MaxItemLineParallelism"/>, the bounded fan-out for per-group processing.</param>
/// <param name="logger">Logger for stock-sync processing.</param>
public sealed class StockSyncSubmittedHandler(
    IItemStockInventoryService itemStockInventoryService,
    IItemStockInventoryExtendedSegmentationService extendedSegmentationService,
    IItemDiscrepencyDetailRepository itemDiscrepencyDetailRepository,
    ISnapshotStockSyncItemRepository snapshotStockSyncItemRepository,
    IItemRepository itemRepository,
    IStockSyncSubmittedOmsPublisher omsPublisher,
    IMessageArchiveWriter archiveWriter,
    IOptions<FeatureFlagsOptions> featureFlagsOptions,
    IOptions<StockSyncSubmittedServiceBusConsumerOptions> consumerOptions,
    ILogger<StockSyncSubmittedHandler> logger)
    : IStockSyncSubmittedHandler
{
    private static readonly HashSet<(DomainEnums.State State, DomainEnums.Status Status)> SellableStates =
    [
        (DomainEnums.State.AVAILABLE, DomainEnums.Status.PREPARED),
        (DomainEnums.State.AVAILABLE, DomainEnums.Status.PICKABLE),
        (DomainEnums.State.INSPECTION, DomainEnums.Status.PICKABLE),
        (DomainEnums.State.AVAILABLETOSELL, DomainEnums.Status.PICKABLE),
    ];

    private static readonly HashSet<(DomainEnums.State State, DomainEnums.Status Status)> NonSellableStates =
    [
        (DomainEnums.State.AVAILABLE, DomainEnums.Status.HELD),
        (DomainEnums.State.INSPECTION, DomainEnums.Status.PICKABLE),
    ];

    /// <inheritdoc/>
    public async Task HandleAsync(StockSyncSubmittedEvent message, string correlationId, CancellationToken cancellationToken)
    {
        var fulfilmentId = message.Location.Id == FulfilmentLocationIds.Brz3PlConsigneeId
            ? FulfilmentLocationIds.BrzDc3PlFulfilmentId
            : message.Location.Id;

        await EnsureItemExistsAsync(message.ProductId, cancellationToken);

        var groups = message.QuantityDetails
            .Where(detail => detail.Domain == StockSyncInventoryDomain.B2C)
            .Where(detail => IsRelevant(detail.State))
            .GroupBy(detail => (detail.CountryOfOrigin, detail.Hallmarking))
            .ToList();

        if (groups.Count == 0)
        {
            return;
        }

        await ForEachGroupAsync(groups, async group =>
            await ProcessGroupAsync(message, fulfilmentId, group.Key.CountryOfOrigin, group.Key.Hallmarking, group, correlationId, cancellationToken),
            cancellationToken);
    }

    private static bool IsRelevant(InventoryEventStateSnapshot state)
    {
        var pair = (InventoryEventStateMapper.ToDomainState(state.State), InventoryEventStateMapper.ToDomainStatus(state.Status));
        return SellableStates.Contains(pair) || NonSellableStates.Contains(pair);
    }

    /// <summary>Assumption 3 - auto-creates a missing item master record, warning rather than failing.</summary>
    private async Task<bool> EnsureItemExistsAsync(string itemCode, CancellationToken cancellationToken)
    {
        var existing = await itemRepository.GetByItemCodeAsync(itemCode, cancellationToken);
        if (existing is not null)
        {
            return true;
        }

        logger.LogWarning("Item master record {ItemCode} not found - auto-creating (docs/events/inventory.StockSyncSubmitted.md assumption 3).", itemCode);
        await itemRepository.CreateAsync(new Item { ItemCode = itemCode }, cancellationToken);

        return false;
    }

    /// <summary>§3.1 group processing: §3.2 sellable + discrepancy, §3.3/§4b non-sellable, §3.4 snapshot (always), §3.5 OMS snapshot.</summary>
    private async Task ProcessGroupAsync(
        StockSyncSubmittedEvent message, string fulfilmentId, string countryOfOrigin, string hallmarking,
        IEnumerable<StockSyncQuantityDetail> details, string correlationId, CancellationToken cancellationToken)
    {
        var detailList = details.ToList();
        var masterDataExists = await itemRepository.GetByItemCodeAsync(message.ProductId, cancellationToken) is not null;

        var sellableResult = await ProcessSellableAsync(
            message, fulfilmentId, countryOfOrigin, hallmarking, detailList, masterDataExists, correlationId, cancellationToken);

        await ProcessNonSellableAsync(fulfilmentId, message.ProductId, countryOfOrigin, hallmarking, detailList, cancellationToken);

        await SaveSnapshotsAsync(message.ProductId, countryOfOrigin, hallmarking, fulfilmentId, detailList, cancellationToken);

        if (sellableResult is not null)
        {
            await PublishOmsSnapshotIfEnabledAsync(message, fulfilmentId, sellableResult.NewB2CAvailable, cancellationToken);
        }
    }

    /// <summary>§3.2/§4.2 - extracts avlPickableQnty/b2BPreparedQty/b2CAvailableToSell, applies the stock-sync Set, and detects/records a discrepancy.</summary>
    private async Task<Application.InventoryEvents.Dtos.ItemStockSyncApplyResult?> ProcessSellableAsync(
        StockSyncSubmittedEvent message, string fulfilmentId, string countryOfOrigin, string hallmarking,
        IReadOnlyList<StockSyncQuantityDetail> details, bool masterDataExists, string correlationId, CancellationToken cancellationToken)
    {
        var sellableDetails = details.Where(detail => SellableStates.Contains(ToPair(detail.State))).ToList();
        if (sellableDetails.Count == 0)
        {
            return null;
        }

        var avlPickableQnty = Normalize(FirstQuantityOrZero(sellableDetails, DomainEnums.Status.PICKABLE, byPickableStatusOnly: true));
        var b2BPreparedQty = Normalize(FirstQuantityOrZero(sellableDetails, DomainEnums.Status.PREPARED, byPickableStatusOnly: false));
        var b2CAvailableToSellDetail = sellableDetails.FirstOrDefault(detail => ToPair(detail.State).State == DomainEnums.State.AVAILABLETOSELL);
        int? b2CAvailableToSell = b2CAvailableToSellDetail is null ? null : Normalize(b2CAvailableToSellDetail.Quantity);

        var beforeJson = JsonSerializer.Serialize(new { fulfilmentId, message.ProductId, countryOfOrigin, hallmarking });
        ArchiveState("stock-sync-before", message.ProductId, beforeJson, correlationId);

        var result = await itemStockInventoryService.ApplyStockSyncAsync(
            fulfilmentId, message.ProductId, countryOfOrigin, hallmarking, avlPickableQnty, b2BPreparedQty, b2CAvailableToSell, cancellationToken);

        if (result.PreviousB2CAvailable != avlPickableQnty)
        {
            var discrepancy = ItemDiscrepencyDetail.Create(
                $"{fulfilmentId}:{message.ProductId}:{hallmarking}:{countryOfOrigin}:{message.SyncDate:O}",
                message.ProductId, countryOfOrigin, hallmarking,
                result.PreviousB2CAvailable, avlPickableQnty, masterDataExists, fulfilmentId);

            await itemDiscrepencyDetailRepository.UpsertAsync(discrepancy, cancellationToken);

            logger.LogWarning(
                "§3.2 discrepancy for {ItemCode}/{FulfilmentId}: IISAvlQty={IISAvlQty}, reported={ReportedQty}.",
                message.ProductId, fulfilmentId, result.PreviousB2CAvailable, avlPickableQnty);
        }

        var afterJson = JsonSerializer.Serialize(new { fulfilmentId, message.ProductId, countryOfOrigin, hallmarking, result.NewB2CAvailable });
        ArchiveState("stock-sync-after", message.ProductId, afterJson, correlationId);

        return result;
    }

    /// <summary>§3.3/§4b - HELD/INSPECTION→PICKABLE lines, one <see cref="IItemStockInventoryExtendedSegmentationService.ApplyAsync"/> call per line; a per-item failure is logged and skipped, not fatal to the message.</summary>
    private async Task ProcessNonSellableAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmarking,
        IReadOnlyList<StockSyncQuantityDetail> details, CancellationToken cancellationToken)
    {
        var nonSellableDetails = details.Where(detail => NonSellableStates.Contains(ToPair(detail.State))).ToList();

        foreach (var detail in nonSellableDetails)
        {
            try
            {
                var (state, status) = ToPair(detail.State);
                var quantity = Normalize(detail.Quantity);

                await extendedSegmentationService.ApplyAsync(
                    fulfilmentId, itemCode, hallmarking, countryOfOrigin,
                    state, status, state, status,
                    quantity, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex,
                    "§3.3 non-sellable line failed for {ItemCode}/{FulfilmentId} - skipping line, message not failed.",
                    itemCode, fulfilmentId);
            }
        }
    }

    /// <summary>§3.4 - always saved, regardless of whether the sellable/discrepancy path changed anything.</summary>
    private async Task SaveSnapshotsAsync(
        string itemCode, string countryOfOrigin, string hallmarking, string fulfilmentId,
        IReadOnlyList<StockSyncQuantityDetail> details, CancellationToken cancellationToken)
    {
        foreach (var detail in details)
        {
            var (state, status) = ToPair(detail.State);
            var quantityType = $"{detail.Domain}.{state}_{status}";

            var snapshot = SnapshotStockSyncItem.Create(
                $"{fulfilmentId}:{itemCode}:{hallmarking}:{countryOfOrigin}:{quantityType}",
                itemCode, countryOfOrigin, fulfilmentId, hallmarking, Normalize(detail.Quantity), quantityType);

            await snapshotStockSyncItemRepository.UpsertAsync(snapshot, cancellationToken);
        }
    }

    /// <summary>§3.5 - feature gate + availability gate + BRZ3PL location round-trip, published after the Cosmos write commits.</summary>
    private async Task PublishOmsSnapshotIfEnabledAsync(
        StockSyncSubmittedEvent message, string fulfilmentId, int b2cAvailableQuantity, CancellationToken cancellationToken)
    {
        var isBrz3Pl = fulfilmentId == FulfilmentLocationIds.BrzDc3PlFulfilmentId;
        var enableSnapShotTowardsOms = !(isBrz3Pl && !featureFlagsOptions.Value.EnableSnapshotTowardsOmsBrz3Pl);

        if (!enableSnapShotTowardsOms)
        {
            return;
        }

        var isCaecom = message.Location.Id == FulfilmentLocationIds.Caecom;
        if (b2cAvailableQuantity <= 0 && !isCaecom)
        {
            return;
        }

        var eventId = $"{fulfilmentId}:{message.ProductId}:{message.SyncDate:O}";

        await omsPublisher.PublishAsync(
            fulfilmentId, message.Location.Type.ToString(), message.ProductId, b2cAvailableQuantity, eventId, cancellationToken);
    }

    private void ArchiveState(string category, string itemCode, string payload, string correlationId) =>
        archiveWriter.Enqueue(MessageArchive.Create($"{category}:{itemCode}:{correlationId}", category, payload, correlationId, DateTime.UtcNow));

    private static (DomainEnums.State State, DomainEnums.Status Status) ToPair(InventoryEventStateSnapshot state) =>
        (InventoryEventStateMapper.ToDomainState(state.State), InventoryEventStateMapper.ToDomainStatus(state.Status));

    /// <summary>§4.2 - FirstOrDefault() on no match yields 0, matching the doc's own extraction rule.</summary>
    private static int FirstQuantityOrZero(IEnumerable<StockSyncQuantityDetail> details, DomainEnums.Status status, bool byPickableStatusOnly) =>
        details.Where(detail => ToPair(detail.State).Status == status).Select(detail => detail.Quantity).FirstOrDefault();

    /// <summary>§4.3 - negative quantities are normalized to 0, never negative in final state.</summary>
    private static int Normalize(int quantity) => Math.Max(0, quantity);

    /// <summary>
    /// Bounded-parallel fan-out (integration-resiliency.instructions.md §6) over one message's
    /// (CountryOfOrigin, Hallmarking) groups - copies <see cref="InventoryAdjusted.Handlers.InventoryAdjustedHandler.ForEachItemLineAsync"/>'s
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
