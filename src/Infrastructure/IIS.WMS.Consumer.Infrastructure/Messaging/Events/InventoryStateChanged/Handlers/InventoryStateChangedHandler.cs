using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Application.OrderTracking;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Rules;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Handlers;

/// <summary>
/// Applies one relayed <see cref="InventoryStateChangedEvent"/> - ported from the upstream Reflex
/// facade's <c>InventoryStateChangedQueueTrigger</c>, excluding its Durable Functions
/// <c>InventoryStateChangedOrchestrator</c>/Activity Trigger dispatch (this service has no Durable Task
/// engine; the Kafka-to-Service-Bus relay pipeline itself, running as its own KEDA-scaled AKS Deployment
/// per kubernetes-deployment-best-practices.instructions.md, is this service's equivalent
/// durability/retry mechanism - see docs/events/inventory.InventoryStateChanged.md). Detects a
/// pick/unpick transition and applies the corresponding inventory mutations per item line via
/// <see cref="IItemStockInventoryExtensionService"/> (ported from Reflex's
/// <c>InventoryPickEventHandler</c>/<c>InventoryUnpickEventHandler</c> with extension calculation);
/// any other transition instead runs §3.3/§3.5/§3.6 (segmentation, extended-inventory segmentation, and
/// B2B adjusted/moved publishing). §3.7 (OMS delta) and §3.8 (ICR snapshot) run per item line
/// regardless of which of the three branches applied - mirroring the reference trigger, whose own
/// OMS-delta/ICR-snapshot checks sit outside/after the three-way pick/unpick/generic branch, and the
/// doc's own §2 flow diagram, which lists OMS Delta Sync (step 8, "Post Pick/Unpick/Segmentation") and
/// ICR Snapshot (step 9) as applying after steps 5/6/7 collectively, not only after step 7. §3.9 (order
/// tracking) runs once per message, only on a pick/unpick transition.
/// </summary>
/// <param name="itemStockInventoryExtensionService">Applies pick/unpick mutations and recalculates B2C extension metrics.</param>
/// <param name="segmentationService">§3.3 inventory segmentation/extension.</param>
/// <param name="extendedSegmentationService">§3.5 extended-inventory segmentation.</param>
/// <param name="inventoryAdjustedOrMovedPublisher">§3.6 B2B adjusted/moved event publisher.</param>
/// <param name="deltaTowardsOmsPublisher">§3.7 OMS delta publisher.</param>
/// <param name="inventoryComparisonReportPublisher">§3.8 ICR snapshot publisher.</param>
/// <param name="orderTrackingPublisher">§3.9 order-tracking publisher.</param>
/// <param name="featureFlagsOptions">Gates for the §3.6/3.7/3.8 downstream publishes.</param>
/// <param name="consumerOptions">Carries <see cref="InventoryStateChangedServiceBusConsumerOptions.MaxItemLineParallelism"/>, the bounded fan-out for per-item-line processing.</param>
/// <param name="logger">Logger for pick/unpick rejects and OMS delta tracking.</param>
public sealed class InventoryStateChangedHandler(
    IItemStockInventoryExtensionService itemStockInventoryExtensionService,
    IItemStockInventorySegmentationService segmentationService,
    IItemStockInventoryExtendedSegmentationService extendedSegmentationService,
    IInventoryAdjustedOrMovedPublisher inventoryAdjustedOrMovedPublisher,
    IDeltaTowardsOmsPublisher deltaTowardsOmsPublisher,
    IInventoryComparisonReportPublisher inventoryComparisonReportPublisher,
    IOrderTrackingPublisher orderTrackingPublisher,
    IOptions<FeatureFlagsOptions> featureFlagsOptions,
    IOptions<InventoryStateChangedServiceBusConsumerOptions> consumerOptions,
    ILogger<InventoryStateChangedHandler> logger)
    : IInventoryStateChangedHandler
{
    /// <inheritdoc/>
    public async Task HandleAsync(InventoryStateChangedEvent message, string correlationId, CancellationToken cancellationToken)
    {
        var isPickEvent = InventoryStateTransitionRules.IsPickableToPrepared(message);
        var isUnpickEvent = InventoryStateTransitionRules.IsUnpickTransition(message);
        await ApplyItemStockMutationsAsync(message, isPickEvent, isUnpickEvent, correlationId, cancellationToken);

        if (!isPickEvent && !isUnpickEvent)
        {
            return;
        }

        var request = new OrderTrackingRelayRequest(
            ReferenceId: message.Id,
            Channel: message.Channel.ToString(),
            FulfilmentUnitId: message.Location.Id,
            FulfilmentUnitType: message.Location.Type.ToString(),
            FunctionName: nameof(InventoryStateChangedHandler),
            OrderId: message.ReferenceId,
            OrderStatus: OrderTrackingStatus.PICKED,
            OrderType: (message.Type == InventoryEventChangeType.PickedB2C ? OrderType.SALES : OrderType.TRANSFER).ToString(),
            Lines: [.. message.ItemLines.Select(item => new OrderTrackingRelayLine(
                ItemCode: item.ProductId,
                CountryOfOrigin: item.CountryOfOrigin,
                HallMarkType: item.Hallmarking,
                Qty: item.Quantity))]);

        await orderTrackingPublisher.PublishAsync(request, cancellationToken);
    }

    /// <summary>
    /// Applies inventory mutations for each item line - mirrors Reflex's orchestrator's own per-item-line loop
    /// (<c>InventoryStateChangedOrchestrator.cs</c>). A pick dispatches on <see cref="InventoryEventChangeType.PickedB2B"/>
    /// / <see cref="InventoryEventChangeType.PickedB2C"/>; an unpick only applies for <see cref="InventoryEventChangeType.Dgp"/>
    /// (mirrors Reflex's <c>InventoryChangeType.DGP</c> guard) - any other type on an unpick transition is logged and
    /// skipped, matching Reflex's "Invalid Type" reject. Anything that is neither a pick nor an unpick runs §3.3/§3.5/§3.6
    /// instead. §3.7/§3.8 run per item line across all three branches - see the type-level remarks.
    /// </summary>
    private async Task ApplyItemStockMutationsAsync(
        InventoryStateChangedEvent message, bool isPickEvent, bool isUnpickEvent, string correlationId, CancellationToken cancellationToken)
    {
        var isThirdPartyLogisticsByType = message.Location.Type == InventoryEventLocationType.ThirdPartyLogistics;
        var isCaecomLocation = message.Location.Id == FulfilmentLocationIds.Caecom;

        if (isPickEvent)
        {
            if (message.Type is not (InventoryEventChangeType.PickedB2B or InventoryEventChangeType.PickedB2C))
            {
                logger.LogWarning(
                    "Pick transition for ReferenceId {ReferenceId}, CorrelationId {CorrelationId} has unsupported Type {Type} - skipping stock mutation.",
                    message.Id, correlationId, message.Type);

                return;
            }

            var isB2BPick = message.Type == InventoryEventChangeType.PickedB2B;

            await ForEachItemLineAsync(message.ItemLines, async item =>
            {
                var deltaResult = isB2BPick
                    ? await itemStockInventoryExtensionService.ApplyPickB2BWithExtensionAsync(
                        message.Location.Id, item.ProductId, item.CountryOfOrigin, item.Hallmarking,
                        item.Quantity, cancellationToken)
                    : await itemStockInventoryExtensionService.ApplyPickB2CWithExtensionAsync(
                        message.Location.Id, item.ProductId, item.CountryOfOrigin, item.Hallmarking,
                        item.Quantity, cancellationToken);

                if (deltaResult.IsB2CChanged)
                {
                    logger.LogInformation(
                        "Pick applied with OMS delta: ReferenceId={ReferenceId}, ItemCode={ItemCode}, Channel={Channel}, Delta={Delta}, CorrelationId={CorrelationId}.",
                        message.Id, item.ProductId, (isB2BPick ? "B2B" : "B2C"), deltaResult.DeltaTowardsOms, correlationId);
                }

                await PublishOmsDeltaAndIcrSnapshotAsync(
                    message, item, deltaResult, isThirdPartyLogisticsByType, isCaecomLocation, cancellationToken);
            }, cancellationToken);

            return;
        }

        if (isUnpickEvent)
        {
            if (message.Type != InventoryEventChangeType.Dgp)
            {
                logger.LogWarning(
                    "Unpick transition for ReferenceId {ReferenceId}, CorrelationId {CorrelationId} has unsupported Type {Type} - skipping stock mutation.",
                    message.Id, correlationId, message.Type);

                return;
            }

            await ForEachItemLineAsync(message.ItemLines, async item =>
            {
                var deltaResult = await itemStockInventoryExtensionService.ApplyUnpickWithExtensionAsync(
                    message.Location.Id, item.ProductId, item.CountryOfOrigin, item.Hallmarking,
                    item.Quantity, cancellationToken);

                if (deltaResult.IsB2CChanged)
                {
                    logger.LogInformation(
                        "Unpick applied with OMS delta: ReferenceId={ReferenceId}, ItemCode={ItemCode}, Delta={Delta}, CorrelationId={CorrelationId}.",
                        message.Id, item.ProductId, deltaResult.DeltaTowardsOms, correlationId);
                }

                await PublishOmsDeltaAndIcrSnapshotAsync(
                    message, item, deltaResult, isThirdPartyLogisticsByType, isCaecomLocation, cancellationToken);
            }, cancellationToken);

            return;
        }

        var isSegmentationTrigger = InventoryStateTransitionRules.IsSegmentationTrigger(message);

        await ForEachItemLineAsync(message.ItemLines, async item =>
        {
            var inboundQty = InventoryChangeTypeSignMapper.GetSignedQuantity(message.Type, item.Quantity);

            ItemStockInventoryDeltaResult? segmentationResult = null;
            if (isSegmentationTrigger)
            {
                segmentationResult = await segmentationService.ApplySegmentationAsync(
                    message.Location.Id, item.ProductId, item.CountryOfOrigin, item.Hallmarking,
                    inboundQty, isThirdPartyLogisticsByType, cancellationToken);
            }

            await extendedSegmentationService.ApplyAsync(
                message.Location.Id, item.ProductId, item.Hallmarking, item.CountryOfOrigin,
                InventoryEventStateMapper.ToDomainState(message.FromState.State),
                InventoryEventStateMapper.ToDomainStatus(message.FromState.Status),
                InventoryEventStateMapper.ToDomainState(message.ToState.State),
                InventoryEventStateMapper.ToDomainStatus(message.ToState.Status),
                inboundQty, cancellationToken);

            await PublishOmsDeltaAndIcrSnapshotAsync(
                message, item, segmentationResult, isThirdPartyLogisticsByType, isCaecomLocation, cancellationToken);
        }, cancellationToken);

        await PublishAdjustedOrMovedIfEnabledAsync(message, isCaecomLocation, cancellationToken);
    }

    /// <summary>
    /// Bounded-parallel fan-out (integration-resiliency.instructions.md §6) over one message's item
    /// lines - each line mutates an independent Cosmos aggregate (<see cref="Domain.Aggregates.ItemStockInventory.BuildId"/>),
    /// so there's no correctness reason to serialize them, only an RU-budget reason to bound
    /// concurrency (<see cref="InventoryStateChangedServiceBusConsumerOptions.MaxItemLineParallelism"/>).
    /// Catches per-item exceptions itself rather than letting them propagate out of the
    /// <c>Parallel.ForEachAsync</c> delegate directly - awaiting that API only ever surfaces a single
    /// exception when multiple iterations fault concurrently (the rest are silently dropped and the
    /// loop short-circuits, leaving remaining item lines unprocessed). Collecting every fault first
    /// guarantees every item line is attempted, then deterministically resurfaces one exception,
    /// prioritizing <see cref="ConcurrencyException"/>/<see cref="OperationCanceledException"/> so
    /// <c>ServiceBusConsumerHostedService.RunProcessMessageAsync</c>'s exception-to-outcome mapping
    /// (integration-resiliency.instructions.md §2) still resolves those to `Abandoned` rather than
    /// dead-lettering on a concurrent, unrelated fault.
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

    /// <summary>
    /// §3.7/§3.8 - runs for every item line regardless of pick/unpick/generic classification. §3.7 only
    /// publishes when <paramref name="deltaResult"/> reports a B2C change and the location-type-dependent
    /// flag (<see cref="FeatureFlagsOptions.EnableDeltaTowardsOms3Pl"/> vs.
    /// <see cref="FeatureFlagsOptions.EnableDeltaTowardsOms"/>) is enabled; §3.8 publishes unconditionally
    /// whenever <see cref="FeatureFlagsOptions.EnableSnapshotForIcr"/> is enabled.
    /// </summary>
    private async Task PublishOmsDeltaAndIcrSnapshotAsync(
        InventoryStateChangedEvent message, InventoryEventItemLine item, ItemStockInventoryDeltaResult? deltaResult,
        bool isThirdPartyLogisticsByType, bool isCaecomLocation, CancellationToken cancellationToken)
    {
        var flags = featureFlagsOptions.Value;

        if (deltaResult is { IsB2CChanged: true })
        {
            var isOmsDeltaEnabled = isThirdPartyLogisticsByType ? flags.EnableDeltaTowardsOms3Pl : flags.EnableDeltaTowardsOms;
            if (isOmsDeltaEnabled)
            {
                await deltaTowardsOmsPublisher.PublishAsync(
                    item.ProductId, message.Location.Id, message.Location.Type.ToString(),
                    item.CountryOfOrigin, item.Hallmarking, deltaResult.DeltaTowardsOms, message.Id, cancellationToken);
            }
        }

        if (flags.EnableSnapshotForIcr)
        {
            await inventoryComparisonReportPublisher.PublishAsync(
                message.Location.Id, item.ProductId, item.Hallmarking, item.CountryOfOrigin,
                isCaecomLocation, cancellationToken);
        }
    }

    /// <summary>
    /// §3.6 - published once per message (all item lines), gated by the three location-based flag
    /// combinations: non-EDC/non-ADC locations under <see cref="FeatureFlagsOptions.EnableDeltaTowardsSap"/>,
    /// the CAECOM (3PL) location under <see cref="FeatureFlagsOptions.EnableDeltaTowardsAx123Pl"/>, or the
    /// ADC location under <see cref="FeatureFlagsOptions.EnableAdcDeltaTowardsAx12"/>.
    /// </summary>
    private async Task PublishAdjustedOrMovedIfEnabledAsync(
        InventoryStateChangedEvent message, bool isCaecomLocation, CancellationToken cancellationToken)
    {
        var flags = featureFlagsOptions.Value;

        var isEnabled =
            (flags.EnableDeltaTowardsSap
                && message.Location.Id != FulfilmentLocationIds.Edc
                && message.Location.Id != FulfilmentLocationIds.Adc)
            || (flags.EnableDeltaTowardsAx123Pl && isCaecomLocation)
            || (flags.EnableAdcDeltaTowardsAx12 && message.Location.Id == FulfilmentLocationIds.Adc);

        if (!isEnabled)
        {
            return;
        }

        var lines = message.ItemLines
            .Select(item => new InventoryAdjustedOrMovedLine
            {
                ItemCode = item.ProductId,
                Qty = item.Quantity,
                CountryOfOrigin = item.CountryOfOrigin,
                Hallmarking = item.Hallmarking,
            })
            .ToList();

        await inventoryAdjustedOrMovedPublisher.PublishAsync(
            message.Channel.ToString(),
            message.Id,
            message.ChangeDate,
            message.Location.Id,
            message.Location.Type.ToString(),
            message.Entity,
            InventoryEventStateMapper.ToDomainState(message.FromState.State),
            InventoryEventStateMapper.ToDomainStatus(message.FromState.Status),
            InventoryEventStateMapper.ToDomainState(message.ToState.State),
            InventoryEventStateMapper.ToDomainStatus(message.ToState.Status),
            message.ReferenceId,
            lines,
            cancellationToken);
    }
}
