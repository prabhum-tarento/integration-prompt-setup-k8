using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Rules;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using DomainEnums = IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryAdjusted.Handlers;

/// <summary>
/// Applies one relayed <see cref="InventoryAdjustedEvent"/> - ported from the upstream Reflex facade's
/// <c>InventoryAdjustedQueueTrigger</c>, excluding its Durable Functions orchestrator dispatch, same as
/// <see cref="InventoryStateChanged.Handlers.InventoryStateChangedHandler"/> (see docs/events/inventory.InventoryAdjusted.md).
/// Unlike that handler, there is no pick/unpick classification and no order-tracking step - this event
/// carries a single <see cref="InventoryEventAdjustment.State"/> snapshot rather than a From/To pair, so
/// every adjustment line always runs the segmentation (§3.2) + extended-state-transition (§3.3) branch,
/// using that one snapshot for both the "from" and "to" side of
/// <see cref="IItemStockInventoryExtendedSegmentationService.ApplyAsync"/> - the from-state decrement is
/// suppressed for this event type regardless (<see cref="ItemStockInventoryExtendedSegmentationService"/>'s
/// own SAE-3032 guard checks <c>ICorrelationContext.Type</c>). §3.4 (OMS delta) and §3.5 (ICR snapshot)
/// run per adjustment line, same as §3.7/§3.8 do for <c>InventoryStateChanged</c>. §3.1 (B2B
/// adjusted/moved) runs once per message, gated by the doc-literal two-flag condition stated in
/// docs/events/inventory.InventoryAdjusted.md §2 step 4 - deliberately not the three-way OR gate
/// <see cref="InventoryStateChanged.Handlers.InventoryStateChangedHandler"/> uses, since this event's own
/// doc states a narrower condition than the shared delta-towards-oms.md doc (which does not list
/// InventoryAdjusted as a consumer).
/// </summary>
/// <param name="segmentationService">§3.2 inventory segmentation/extension.</param>
/// <param name="extendedSegmentationService">§3.3 extended-inventory segmentation.</param>
/// <param name="inventoryAdjustedOrMovedPublisher">§3.1 B2B adjusted/moved event publisher.</param>
/// <param name="deltaTowardsOmsPublisher">§3.4 OMS delta publisher.</param>
/// <param name="inventoryComparisonReportPublisher">§3.5 ICR snapshot publisher.</param>
/// <param name="featureFlagsOptions">Gates for the §3.1/3.4/3.5 downstream publishes.</param>
/// <param name="consumerOptions">Carries <see cref="InventoryAdjustedServiceBusConsumerOptions.MaxItemLineParallelism"/>, the bounded fan-out for per-adjustment-line processing.</param>
/// <param name="logger">Logger for OMS delta tracking.</param>
public sealed class InventoryAdjustedHandler(
    IItemStockInventorySegmentationService segmentationService,
    IItemStockInventoryExtendedSegmentationService extendedSegmentationService,
    IInventoryAdjustedOrMovedPublisher inventoryAdjustedOrMovedPublisher,
    IDeltaTowardsOmsPublisher deltaTowardsOmsPublisher,
    IInventoryComparisonReportPublisher inventoryComparisonReportPublisher,
    IOptions<FeatureFlagsOptions> featureFlagsOptions,
    IOptions<InventoryAdjustedServiceBusConsumerOptions> consumerOptions,
    ILogger<InventoryAdjustedHandler> logger)
    : IInventoryAdjustedHandler
{
    /// <inheritdoc/>
    public async Task HandleAsync(InventoryAdjustedEvent message, string correlationId, CancellationToken cancellationToken)
    {
        var adjustment = message.Adjustment;
        var isThirdPartyLogisticsByType = adjustment.Location.Type == InventoryEventLocationType.ThirdPartyLogistics;
        var isCaecomLocation = adjustment.Location.Id == FulfilmentLocationIds.Caecom;
        var isSegmentationTrigger = InventoryStateTransitionRules.IsAvailablePickable(adjustment.State);

        var state = InventoryEventStateMapper.ToDomainState(adjustment.State.State);
        var status = InventoryEventStateMapper.ToDomainStatus(adjustment.State.Status);

        await ForEachItemLineAsync(adjustment.AdjustmentLines, async item =>
        {
            ItemStockInventoryDeltaResult? segmentationResult = null;
            if (isSegmentationTrigger)
            {
                segmentationResult = await segmentationService.ApplySegmentationAsync(
                    adjustment.Location.Id, item.ProductId, item.CountryOfOrigin, item.Hallmarking,
                    item.Quantity, isThirdPartyLogisticsByType, cancellationToken);
            }

            await extendedSegmentationService.ApplyAsync(
                adjustment.Location.Id, item.ProductId, item.Hallmarking, item.CountryOfOrigin,
                state, status, state, status,
                item.Quantity, cancellationToken);

            await PublishOmsDeltaAndIcrSnapshotAsync(
                message, item, segmentationResult, isThirdPartyLogisticsByType, isCaecomLocation, cancellationToken);
        }, cancellationToken);

        await PublishAdjustedOrMovedIfEnabledAsync(message, cancellationToken);
    }

    /// <summary>
    /// Bounded-parallel fan-out (integration-resiliency.instructions.md §6) over one message's
    /// adjustment lines - copies <see cref="InventoryStateChanged.Handlers.InventoryStateChangedHandler.ForEachItemLineAsync"/>'s
    /// exception-collection/prioritization exactly (see that method's own remarks for why).
    /// </summary>
    private async Task ForEachItemLineAsync(
        IReadOnlyCollection<InventoryEventItemLine> itemLines,
        Func<InventoryEventItemLine, Task> processItemAsync,
        CancellationToken cancellationToken)
    {
        var exceptions = new ConcurrentQueue<Exception>();

        await Parallel.ForEachAsync(
            itemLines,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = consumerOptions.Value.MaxItemLineParallelism,
                CancellationToken = cancellationToken,
            },
            async (item, _) =>
            {
                try
                {
                    await processItemAsync(item);
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

    /// <summary>§3.4/§3.5 - runs for every adjustment line, same gating as <see cref="InventoryStateChanged.Handlers.InventoryStateChangedHandler.PublishOmsDeltaAndIcrSnapshotAsync"/>.</summary>
    private async Task PublishOmsDeltaAndIcrSnapshotAsync(
        InventoryAdjustedEvent message, InventoryEventItemLine item, ItemStockInventoryDeltaResult? deltaResult,
        bool isThirdPartyLogisticsByType, bool isCaecomLocation, CancellationToken cancellationToken)
    {
        var flags = featureFlagsOptions.Value;
        var adjustment = message.Adjustment;

        if (deltaResult is { IsB2CChanged: true })
        {
            var isOmsDeltaEnabled = isThirdPartyLogisticsByType ? flags.EnableDeltaTowardsOms3Pl : flags.EnableDeltaTowardsOms;
            if (isOmsDeltaEnabled)
            {
                await deltaTowardsOmsPublisher.PublishAsync(
                    item.ProductId, adjustment.Location.Id, adjustment.Location.Type.ToString(),
                    item.CountryOfOrigin, item.Hallmarking, deltaResult.DeltaTowardsOms, adjustment.ReferenceId, cancellationToken);
            }
        }

        if (flags.EnableSnapshotForIcr)
        {
            await inventoryComparisonReportPublisher.PublishAsync(
                adjustment.Location.Id, item.ProductId, item.Hallmarking, item.CountryOfOrigin,
                isCaecomLocation, cancellationToken);
        }
    }

    /// <summary>
    /// §3.1 - published once per message (all adjustment lines), gated by the doc-literal two-flag
    /// condition (docs/events/inventory.InventoryAdjusted.md §2 step 4):
    /// <see cref="FeatureFlagsOptions.EnableDeltaTowardsSap"/> AND (location isn't ADC, OR
    /// <see cref="FeatureFlagsOptions.EnableAdcDeltaTowardsAx12"/> is also enabled).
    /// </summary>
    private async Task PublishAdjustedOrMovedIfEnabledAsync(InventoryAdjustedEvent message, CancellationToken cancellationToken)
    {
        var flags = featureFlagsOptions.Value;
        var adjustment = message.Adjustment;

        var isEnabled = flags.EnableDeltaTowardsSap
            && (adjustment.Location.Id != FulfilmentLocationIds.Adc || flags.EnableAdcDeltaTowardsAx12);

        if (!isEnabled)
        {
            return;
        }

        var isNegativeQuantity = adjustment.AdjustmentLines.Sum(line => line.Quantity) < 0;

        var toState = isNegativeQuantity ? DomainEnums.State.UNKNOWN : InventoryEventStateMapper.ToDomainState(adjustment.State.State);
        var toStatus = isNegativeQuantity ? DomainEnums.Status.UNKNOWN : InventoryEventStateMapper.ToDomainStatus(adjustment.State.Status);
        var fromState = InventoryEventStateMapper.ToDomainState(adjustment.State.State);
        var fromStatus = InventoryEventStateMapper.ToDomainStatus(adjustment.State.Status);

        var lines = adjustment.AdjustmentLines
            .Select(item => new InventoryAdjustedOrMovedLine
            {
                ItemCode = item.ProductId,
                Qty = item.Quantity,
                CountryOfOrigin = item.CountryOfOrigin,
                Hallmarking = item.Hallmarking,
                Reason = adjustment.Reason.ToString(),
            })
            .ToList();

        await inventoryAdjustedOrMovedPublisher.PublishAsync(
            message.Channel.ToString(),
            adjustment.ReferenceId,
            adjustment.AdjustmentDate,
            adjustment.Location.Id,
            adjustment.Location.Type.ToString(),
            adjustment.Entity,
            fromState,
            fromStatus,
            toState,
            toStatus,
            adjustment.ReferenceId,
            lines,
            cancellationToken);
    }
}
