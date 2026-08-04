using IIS.WMS.Consumer.Application.InternalHallmarkingStatusChanged;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Application.OrderTracking;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged.Handlers;

/// <summary>
/// Applies one relayed <see cref="InternalHallmarkingStatusChangedEvent"/>
/// (docs/events/inventory.InternalHallmarkingStatusChanged.md §2/§3) - dispatches on the event's own
/// top-level <see cref="Status"/> to the matching <see cref="IInternalHallmarkingStatusChangedService"/>
/// use case (STARTED/PICKED/CHANGED/FINISHED); an unrecognized status is logged and skipped entirely,
/// including its order-tracking publish (doc flowchart's <c>DefaultPath --&gt; Complete</c> edge). Unlike
/// <see cref="InventoryStateChanged.Handlers.InventoryStateChangedHandler"/>, which only publishes
/// order-tracking on a pick/unpick transition, every one of the four recognized statuses here publishes
/// order-tracking unconditionally (doc flowchart: all four branches converge on
/// <c>OrderTrack --&gt; Complete</c>) - even STARTED's <c>MISSING_INVENTORY</c> rejection still reaches it,
/// since transit bookkeeping and order-tracking are independent of whether the inventory Patch itself
/// applied (doc §3.1/§3.5, flowchart's <c>Missing1 --&gt; Transit1</c> edge). This event carries a single
/// <see cref="HallmarkingItemLine"/>, not a collection, so there is no per-item-line fan-out.
/// </summary>
/// <param name="internalHallmarkingStatusChangedService">Status-routed use-case orchestration (§3.1-§3.5), including the §3.5 in-transit bookkeeping run as normal handler logic.</param>
/// <param name="orderTrackingPublisher">§8 order-tracking publisher, run for every recognized status.</param>
/// <param name="deltaTowardsOmsPublisher">OMS delta publisher (`nexus-producer`), gated by <see cref="FeatureFlagsOptions.EnableDeltaTowardsOms"/> and each use case's own <see cref="ItemStockInventoryDeltaResult.IsB2CChanged"/>.</param>
/// <param name="inventoryComparisonReportPublisher">ICR snapshot publisher, gated by <see cref="FeatureFlagsOptions.EnableSnapshotForIcr"/> - runs for PICKED (§3.2) and CHANGED (§3.4 step 7) only, per doc text (STARTED/FINISHED don't mention it).</param>
/// <param name="inventoryAdjustedReflexPublisher">FINISHED-only `inventory-adjusted-reflex` publisher (§3.5/§9).</param>
/// <param name="featureFlagsOptions">Gates for the OMS-delta/ICR-snapshot downstream publishes.</param>
/// <param name="logger">Logger for unrecognized-status skips.</param>
public sealed class InternalHallmarkingStatusChangedHandler(
    IInternalHallmarkingStatusChangedService internalHallmarkingStatusChangedService,
    IOrderTrackingPublisher orderTrackingPublisher,
    IDeltaTowardsOmsPublisher deltaTowardsOmsPublisher,
    IInventoryComparisonReportPublisher inventoryComparisonReportPublisher,
    IInventoryAdjustedReflexPublisher inventoryAdjustedReflexPublisher,
    IOptions<FeatureFlagsOptions> featureFlagsOptions,
    ILogger<InternalHallmarkingStatusChangedHandler> logger)
    : IInternalHallmarkingStatusChangedHandler
{
    /// <inheritdoc/>
    public async Task HandleAsync(InternalHallmarkingStatusChangedEvent message, string correlationId, CancellationToken cancellationToken)
    {
        var orderTrackingStatus = ToOrderTrackingStatus(message.Status);

        if (orderTrackingStatus is null)
        {
            logger.LogWarning(
                "Unrecognized internal-hallmarking status {Status} for Id {Id}, CorrelationId {CorrelationId} - skipping.",
                message.Status, message.Id, correlationId);

            return;
        }

        switch (message.Status)
        {
            case Status.Started:
                await HandleStartedAsync(message, cancellationToken);
                break;
            case Status.Picked:
                await HandlePickedAsync(message, cancellationToken);
                break;
            case Status.Changed:
                await HandleChangedAsync(message, cancellationToken);
                break;
            case Status.Finished:
                await HandleFinishedAsync(message, cancellationToken);
                break;
        }

        await PublishOrderTrackingAsync(message, orderTrackingStatus.Value, cancellationToken);
    }

    private static OrderTrackingStatus? ToOrderTrackingStatus(Status status) => status switch
    {
        Status.Started => OrderTrackingStatus.ALLOCATED,
        Status.Picked => OrderTrackingStatus.PICKED,
        Status.Changed => OrderTrackingStatus.INTRANSIT,
        Status.Finished => OrderTrackingStatus.SHIPPED,
        _ => null,
    };

    /// <summary>§3.1 STARTED - allocate, then publish the OMS delta if enabled (doc flowchart: no ICR snapshot on this path).</summary>
    private async Task HandleStartedAsync(InternalHallmarkingStatusChangedEvent message, CancellationToken cancellationToken)
    {
        var itemLine = message.ItemLine;

        var deltaResult = await internalHallmarkingStatusChangedService.AllocateAsync(
            message.Location.Id, itemLine.ProductId, itemLine.CountryOfOrigin, itemLine.HallmarkingTo,
            itemLine.Quantity, cancellationToken);

        await PublishOmsDeltaIfEnabledAsync(message, itemLine, itemLine.HallmarkingTo, deltaResult, cancellationToken);
    }

    /// <summary>§3.2/§3.3 PICKED - pick and ship, then the ICR snapshot (if enabled) followed by the OMS delta (if enabled), per doc flowchart's <c>Snap --&gt; Delta2</c> ordering.</summary>
    private async Task HandlePickedAsync(InternalHallmarkingStatusChangedEvent message, CancellationToken cancellationToken)
    {
        var itemLine = message.ItemLine;

        var deltaResult = await internalHallmarkingStatusChangedService.PickAndShipAsync(
            message.Location.Id, itemLine.ProductId, itemLine.CountryOfOrigin, itemLine.HallmarkingTo,
            itemLine.Quantity, cancellationToken);

        await PublishIcrSnapshotIfEnabledAsync(message, itemLine, itemLine.HallmarkingTo, cancellationToken);
        await PublishOmsDeltaIfEnabledAsync(message, itemLine, itemLine.HallmarkingTo, deltaResult, cancellationToken);
    }

    /// <summary>§3.4 CHANGED - move inventory between hallmark legs, then the OMS delta followed by the ICR snapshot (if enabled), per doc §3.4 steps 5/7 ordering.</summary>
    private async Task HandleChangedAsync(InternalHallmarkingStatusChangedEvent message, CancellationToken cancellationToken)
    {
        var itemLine = message.ItemLine;
        var isThirdPartyLogisticsByType = message.Location.Type == InventoryEventLocationType.ThirdPartyLogistics;

        var deltaResult = await internalHallmarkingStatusChangedService.ChangeHallmarkAsync(
            message.Location.Id, itemLine.ProductId, itemLine.CountryOfOrigin, itemLine.HallmarkingFrom, itemLine.HallmarkingTo,
            itemLine.Quantity, isThirdPartyLogisticsByType, cancellationToken);

        await PublishOmsDeltaIfEnabledAsync(message, itemLine, itemLine.HallmarkingTo, deltaResult, cancellationToken);
        await PublishIcrSnapshotIfEnabledAsync(message, itemLine, itemLine.HallmarkingTo, cancellationToken);
    }

    /// <summary>§3.5 FINISHED - complete the transit, then unconditionally publish to `inventory-adjusted-reflex` (doc §3.5/§9 - no feature-flag gate on this path).</summary>
    private async Task HandleFinishedAsync(InternalHallmarkingStatusChangedEvent message, CancellationToken cancellationToken)
    {
        var itemLine = message.ItemLine;

        await internalHallmarkingStatusChangedService.CompleteTransitAsync(
            message.Location.Id, itemLine.ProductId, itemLine.CountryOfOrigin, itemLine.HallmarkingTo,
            itemLine.Quantity, cancellationToken);

        var toState = InventoryEventStateMapper.ToDomainState(message.InventoryState.State);
        var toStatus = InventoryEventStateMapper.ToDomainStatus(message.InventoryState.Status);

        await inventoryAdjustedReflexPublisher.PublishAsync(
            message.Channel.ToString(),
            message.Id,
            message.ChangeDate,
            message.Location.Id,
            message.Location.Type.ToString(),
            message.Entity,
            itemLine.ProductId,
            itemLine.Quantity,
            itemLine.CountryOfOrigin,
            itemLine.HallmarkingTo,
            toState,
            toStatus,
            message.Id,
            cancellationToken);
    }

    private async Task PublishOmsDeltaIfEnabledAsync(
        InternalHallmarkingStatusChangedEvent message, HallmarkingItemLine itemLine, string hallmark,
        ItemStockInventoryDeltaResult deltaResult, CancellationToken cancellationToken)
    {
        if (deltaResult is not { IsB2CChanged: true } || !featureFlagsOptions.Value.EnableDeltaTowardsOms)
        {
            return;
        }

        await deltaTowardsOmsPublisher.PublishAsync(
            itemLine.ProductId, message.Location.Id, message.Location.Type.ToString(), itemLine.CountryOfOrigin, hallmark,
            deltaResult.DeltaTowardsOms, message.Id, cancellationToken);
    }

    /// <summary>
    /// The published event's own <c>Location.Type</c> is driven by whether this is the CAECOM (3PL)
    /// location specifically (<see cref="FulfilmentLocationIds.Caecom"/>), not the wire event's own
    /// <see cref="InventoryEventLocationType.ThirdPartyLogistics"/> location-type flag - same distinction
    /// <see cref="InventoryStateChanged.Handlers.InventoryStateChangedHandler.PublishOmsDeltaAndIcrSnapshotAsync"/>
    /// draws between its own <c>isCaecomLocation</c> and <c>isThirdPartyLogisticsByType</c> locals.
    /// </summary>
    private async Task PublishIcrSnapshotIfEnabledAsync(
        InternalHallmarkingStatusChangedEvent message, HallmarkingItemLine itemLine, string hallmark, CancellationToken cancellationToken)
    {
        if (!featureFlagsOptions.Value.EnableSnapshotForIcr)
        {
            return;
        }

        var isCaecomLocation = message.Location.Id == FulfilmentLocationIds.Caecom;

        await inventoryComparisonReportPublisher.PublishAsync(
            message.Location.Id, itemLine.ProductId, hallmark, itemLine.CountryOfOrigin, isCaecomLocation, cancellationToken);
    }

    private async Task PublishOrderTrackingAsync(
        InternalHallmarkingStatusChangedEvent message, OrderTrackingStatus orderTrackingStatus, CancellationToken cancellationToken)
    {
        var itemLine = message.ItemLine;

        var request = new OrderTrackingRelayRequest(
            ReferenceId: message.Id,
            Channel: message.Channel.ToString(),
            FulfilmentUnitId: message.Location.Id,
            FulfilmentUnitType: message.Location.Type.ToString(),
            FunctionName: nameof(InternalHallmarkingStatusChangedHandler),
            OrderId: message.Id,
            OrderStatus: orderTrackingStatus,
            OrderType: OrderType.INTERNALHALLMARKING.ToString(),
            Lines:
            [
                new OrderTrackingRelayLine(
                    ItemCode: itemLine.ProductId,
                    CountryOfOrigin: itemLine.CountryOfOrigin,
                    HallMarkType: itemLine.HallmarkingTo,
                    Qty: itemLine.Quantity),
            ]);

        await orderTrackingPublisher.PublishAsync(request, cancellationToken);
    }
}
