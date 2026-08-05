using System.Text.Json;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.OrderTracking;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderToInventoryAllocated.Handlers;

/// <summary>
/// Applies one relayed <see cref="OrderToInventoryAllocatedEvent"/> (docs/events/inventory.OrderToInventoryAllocated.md).
/// Per §3.1-§3.8: performs B2B/B2C allocation, B2C extension recalculation when extended,
/// item-level segmentation, archives before/after snapshots, and publishes ICR/OMS-delta/order-tracking
/// notifications per feature-flag and delta-detection gates.
/// </summary>
/// <param name="orderToInventoryAllocatedService">§3.2-§3.5 allocation and extension logic.</param>
/// <param name="deltaTowardsOmsPublisher">§3.7 OMS delta publisher (feature-flag gated).</param>
/// <param name="inventoryComparisonReportPublisher">§3.8 ICR snapshot publisher (feature-flag gated).</param>
/// <param name="orderTrackingPublisher">§3.9 order-tracking request publisher (always attempted, best-effort).</param>
/// <param name="archiveWriter">Message archive background writer (best-effort).</param>
/// <param name="featureFlagsOptions">Feature flag options for delta/ICR publishing gates.</param>
/// <param name="logger">Logger for processing events.</param>
public sealed class OrderToInventoryAllocatedHandler(
    IOrderToInventoryAllocatedService orderToInventoryAllocatedService,
    IDeltaTowardsOmsPublisher deltaTowardsOmsPublisher,
    IInventoryComparisonReportPublisher inventoryComparisonReportPublisher,
    IOrderTrackingPublisher orderTrackingPublisher,
    IMessageArchiveWriter archiveWriter,
    IOptions<FeatureFlagsOptions> featureFlagsOptions,
    ILogger<OrderToInventoryAllocatedHandler> logger) : IOrderToInventoryAllocatedHandler
{
    private const string EventTypeName = "Inventory_OrderToInventoryAllocated";

    public async Task HandleAsync(OrderToInventoryAllocatedEvent message, string correlationId, CancellationToken cancellationToken)
    {
        var isThirdPartyLogistics = message.Location.Type == InventoryStateChanged.InventoryEventLocationType.ThirdPartyLogistics;

        // Archive BEFORE (best-effort, non-blocking)
        archiveWriter.Enqueue(MessageArchive.Create(
            $"{EventTypeName}:{message.ReferenceId}",
            EventTypeName,
            JsonSerializer.Serialize(message),
            correlationId,
            DateTime.UtcNow));

        // Map wire enum to domain enum
        var orderDomain = MapOrderDomain(message.OrderDomain);

        // §3.2-§3.5: Allocation + extension + segmentation
        var deltaResult = await orderToInventoryAllocatedService.AllocateAsync(
            message.Location.Id,
            message.ProductId,
            message.CountryOfOrigin,
            message.Hallmarking,
            orderDomain,
            message.AllocatedFromB2BBucketQuantity,
            message.AllocatedFromB2CBucketQuantity,
            isThirdPartyLogistics,
            cancellationToken);

        logger.LogInformation(
            "ORDER_ALLOCATION_COMPLETED: ReferenceId={ReferenceId}, ProductId={ProductId}, B2B={B2B}, B2C={B2C}, Delta={Delta}, CorrelationId={CorrelationId}.",
            message.ReferenceId, message.ProductId, message.AllocatedFromB2BBucketQuantity, message.AllocatedFromB2CBucketQuantity, deltaResult.DeltaTowardsOms, correlationId);

        // §3.7: OMS delta (feature-gated)
        if (deltaResult.IsB2CChanged)
        {
            var enableOmsDelta = isThirdPartyLogistics
                ? featureFlagsOptions.Value.EnableDeltaTowardsOms3Pl
                : featureFlagsOptions.Value.EnableDeltaTowardsOms;

            if (enableOmsDelta)
            {
                await deltaTowardsOmsPublisher.PublishAsync(
                    message.ProductId,
                    message.Location.Id,
                    message.Location.Type.ToString(),
                    message.CountryOfOrigin,
                    message.Hallmarking,
                    deltaResult.DeltaTowardsOms,
                    message.ReferenceId,
                    cancellationToken);
            }
        }

        // §3.8: ICR snapshot (feature-gated)
        if (featureFlagsOptions.Value.EnableSnapshotForIcr)
        {
            await inventoryComparisonReportPublisher.PublishAsync(
                message.Location.Id,
                message.ProductId,
                message.Hallmarking,
                message.CountryOfOrigin,
                isThirdPartyLogistics,
                cancellationToken);
        }

        // §3.9: Order tracking (best-effort, always attempted)
        var trackingOrderType = MapOrderType(orderDomain);
        var trackingRequest = new OrderTrackingRelayRequest(
            ReferenceId: message.ReferenceId,
            Channel: message.Channel.ToString(),
            FulfilmentUnitId: message.Location.Id,
            FulfilmentUnitType: message.Location.Type.ToString(),
            FunctionName: nameof(OrderToInventoryAllocatedHandler),
            OrderId: message.OrderId,
            OrderStatus: OrderTrackingStatus.ALLOCATED,
            OrderType: trackingOrderType,
            Lines: [new OrderTrackingRelayLine(
                ItemCode: message.ProductId,
                CountryOfOrigin: message.CountryOfOrigin,
                HallMarkType: message.Hallmarking,
                Qty: message.AllocatedFromB2BBucketQuantity + message.AllocatedFromB2CBucketQuantity)]);

        await orderTrackingPublisher.PublishAsync(trackingRequest, cancellationToken);

        // Archive AFTER (best-effort, non-blocking)
        archiveWriter.Enqueue(MessageArchive.Create(
            $"{EventTypeName}:after:{message.ReferenceId}",
            $"{EventTypeName}:after",
            JsonSerializer.Serialize(new { message.ReferenceId, deltaResult }),
            correlationId,
            DateTime.UtcNow));
    }

    private static OrderDomain MapOrderDomain(InventoryDomainType source) =>
        source switch
        {
            InventoryDomainType.B2B => OrderDomain.B2B,
            InventoryDomainType.B2C => OrderDomain.B2C,
            InventoryDomainType.InternalHallmarking => OrderDomain.InternalHallmarking,
            InventoryDomainType.ExternalHallmarking => OrderDomain.ExternalHallmarking,
            InventoryDomainType.Omni => OrderDomain.Omni,
            _ => OrderDomain.Unknown,
        };

    private static string MapOrderType(OrderDomain orderDomain) =>
        orderDomain switch
        {
            OrderDomain.B2B => "TRANSFER",
            OrderDomain.B2C => "SALES",
            OrderDomain.InternalHallmarking => "INTERNALHALLMARKING",
            OrderDomain.ExternalHallmarking => "EXTERNALHALLMARKING",
            _ => "UNKNOWN",
        };
}
