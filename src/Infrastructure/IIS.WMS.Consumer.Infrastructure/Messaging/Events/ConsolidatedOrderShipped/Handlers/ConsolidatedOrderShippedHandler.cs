using System.Text.Json;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Application.OrderTracking;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped.Handlers;

/// <summary>
/// Applies one relayed <see cref="ConsolidatedOrderShippedEvent"/> (docs/events/b2b.sales.ConsolidatedOrderShipped.md
/// §2/§3). Per shipment line (processed sequentially): confirms the B2B/PSC inventory buckets via
/// <see cref="IConsolidatedOrderShippedService"/> (§3.1/§4.1), publishes the OMS delta when eligible
/// (§3.1 step 7), applies item-level segmentation (§4), and publishes the ICR snapshot when eligible
/// (§5). Once per message: runs the DEECOMDC e-commerce engraving branch (§3.3), then groups lines by
/// <see cref="ConsolidatedOrderShippedRules.ResolveGroupKey"/> and publishes one order-tracking request
/// per eligible group (§3.2).
///
/// <remarks>
/// Data protection (doc §9): <c>CustomerId</c> is validated before use and never logged - only the
/// match/mismatch outcome is logged, never the raw resolved value.
/// </remarks>
/// </summary>
/// <param name="consolidatedOrderShippedService">§3.1/§4.1 B2B confirmation inventory logic.</param>
/// <param name="itemStockWarehouseInventoryService">§3.3 step 3 DEECOMDC engraving warehouse-stock logic.</param>
/// <param name="itemStockInventorySegmentationService">§4/§2 step 4 item-level segmentation.</param>
/// <param name="orderTrackingRepository">§3.3 step 1 OrderTracking read for CustomerId resolution.</param>
/// <param name="ecomCustomerRepository">§3.3 step 1 ECOMDCLIST/TDCCustomerId reference-data lookup.</param>
/// <param name="deltaTowardsOmsPublisher">§3.1 step 7 OMS delta publisher (feature-flag gated).</param>
/// <param name="inventoryComparisonReportPublisher">§5 ICR snapshot publisher (feature-flag gated).</param>
/// <param name="orderTrackingPublisher">§3.2 order-tracking publisher (always attempted, best-effort).</param>
/// <param name="archiveWriter">Message archive background writer (best-effort).</param>
/// <param name="featureFlagsOptions">Feature flag options for the OMS delta / ICR snapshot publish gates - this event's own §9 flag table names only <c>EnableDeltaTowardsOms</c>/<c>EnableSnapshotForIcr</c>, no 3PL variant.</param>
/// <param name="logger">Logger for per-line and overall processing events.</param>
public sealed class ConsolidatedOrderShippedHandler(
    IConsolidatedOrderShippedService consolidatedOrderShippedService,
    IItemStockWarehouseInventoryService itemStockWarehouseInventoryService,
    IItemStockInventorySegmentationService itemStockInventorySegmentationService,
    IOrderTrackingRepository orderTrackingRepository,
    IEcomCustomerRepository ecomCustomerRepository,
    IDeltaTowardsOmsPublisher deltaTowardsOmsPublisher,
    IInventoryComparisonReportPublisher inventoryComparisonReportPublisher,
    IOrderTrackingPublisher orderTrackingPublisher,
    IMessageArchiveWriter archiveWriter,
    IOptions<FeatureFlagsOptions> featureFlagsOptions,
    ILogger<ConsolidatedOrderShippedHandler> logger) : IConsolidatedOrderShippedHandler
{
    private const string EventTypeName = "b2b.sales.ConsolidatedOrderShipped";
    private const string DeecomdcCustomerId = "DEECOMDC";

    /// <inheritdoc/>
    public async Task HandleAsync(ConsolidatedOrderShippedEvent message, string correlationId, CancellationToken cancellationToken)
    {
        var shipment = message.Shipment;
        var warehouseCode = shipment.WarehouseCode;
        var archiveKey = $"{EventTypeName}:{shipment.Id}";

        // Archive BEFORE (best-effort, non-blocking)
        archiveWriter.Enqueue(MessageArchive.Create(
            archiveKey,
            EventTypeName,
            JsonSerializer.Serialize(message),
            correlationId,
            DateTime.UtcNow));

        foreach (var line in shipment.ShipmentLines)
        {
            await ConfirmLineAsync(warehouseCode, shipment.ConfirmationType, line, cancellationToken);
        }

        await ApplyEcomEngravingAsync(message, correlationId, cancellationToken);
        await PublishOrderTrackingAsync(message, cancellationToken);

        // Archive AFTER (best-effort, non-blocking)
        archiveWriter.Enqueue(MessageArchive.Create(
            $"{archiveKey}:after",
            $"{EventTypeName}:after",
            JsonSerializer.Serialize(new { shipment.Id, warehouseCode }),
            correlationId,
            DateTime.UtcNow));
    }

    /// <summary>§3.1/§4.1 B2B confirmation, §3.1 step 7 OMS delta, §4/§2 step 4 segmentation, §5 ICR snapshot - for one shipment line.</summary>
    private async Task ConfirmLineAsync(
        string warehouseCode, Domain.Enums.ConfirmationType confirmationType, ConsolidatedOrderShipmentLine line, CancellationToken cancellationToken)
    {
        var countryOfOrigin = line.CountryOfOrigin ?? "UNKNOWN";
        var hallmark = line.Hallmarking ?? "NON";
        var isThirdPartyLogistics = ConsolidatedOrderShippedRules.IsNotTdcOrAdc(warehouseCode);

        var request = new B2BOrderConfirmedRequest(
            FulfilmentCode: warehouseCode,
            ItemCode: line.ProductId,
            CountryOfOrigin: countryOfOrigin,
            Hallmark: hallmark,
            ShippedQuantity: line.Quantity,
            ConfirmationType: confirmationType,
            AllocatedFromB2BBucketQuantity: line.AllocatedFromB2BBucketQuantity ?? 0);

        var result = await consolidatedOrderShippedService.ConfirmAsync(request, cancellationToken);

        // §3.1 step 7: OMS delta - this event's own §9 flag table names only EnableDeltaTowardsOms (no 3PL variant)
        if (result.IsB2CChanged && featureFlagsOptions.Value.EnableDeltaTowardsOms)
        {
            await deltaTowardsOmsPublisher.PublishAsync(
                line.ProductId,
                warehouseCode,
                InventoryStateChanged.InventoryEventLocationType.Warehouse.ToString(),
                countryOfOrigin,
                hallmark,
                result.DeltaTowardsOms,
                line.LineNum,
                cancellationToken);
        }

        // §2 step 4 / §4: item-level segmentation
        await itemStockInventorySegmentationService.ApplySegmentationAsync(
            warehouseCode, line.ProductId, countryOfOrigin, hallmark, line.Quantity, isThirdPartyLogistics, cancellationToken);

        // §5: ICR snapshot - this event's own §9 flag table names only EnableSnapshotForIcr
        if (featureFlagsOptions.Value.EnableSnapshotForIcr)
        {
            await inventoryComparisonReportPublisher.PublishAsync(
                warehouseCode, line.ProductId, hallmark, countryOfOrigin, isThirdPartyLogistics, cancellationToken);
        }
    }

    /// <summary>
    /// §3.3 DEECOMDC e-commerce engraving workflow - runs once per message, not per line.
    /// </summary>
    private async Task ApplyEcomEngravingAsync(ConsolidatedOrderShippedEvent message, string correlationId, CancellationToken cancellationToken)
    {
        var shipment = message.Shipment;
        var trackingId = $"{shipment.WarehouseCode}:{message.ParentOrderId}";
        var tracking = await orderTrackingRepository.GetAsync(trackingId, trackingId, cancellationToken);

        if (tracking?.CustomerId is null)
        {
            logger.LogInformation(
                "CONSOLIDATED_ORDER_SHIPPED_ECOM_CUSTOMER_ID_EMPTY: ParentOrderId={ParentOrderId}, WarehouseCode={WarehouseCode}, CorrelationId={CorrelationId}.",
                message.ParentOrderId, shipment.WarehouseCode, correlationId);
            return;
        }

        var ecomCustomer = await ecomCustomerRepository.GetAsync(shipment.WarehouseCode, cancellationToken);
        var isMatch = ecomCustomer is not null && ecomCustomer.Matches(tracking.CustomerId);

        if (!isMatch)
        {
            logger.LogInformation(
                "CONSOLIDATED_ORDER_SHIPPED_ECOM_CUSTOMER_MISMATCH: ParentOrderId={ParentOrderId}, WarehouseCode={WarehouseCode}, CorrelationId={CorrelationId}.",
                message.ParentOrderId, shipment.WarehouseCode, correlationId);
            return;
        }

        foreach (var line in shipment.ShipmentLines)
        {
            await itemStockWarehouseInventoryService.ApplyShipmentAsync(
                shipment.WarehouseCode, line.ProductId, line.Quantity, cancellationToken);
        }
    }

    /// <summary>§3.2 order-tracking request building - one request per resolved group key, published only for eligible lines.</summary>
    private async Task PublishOrderTrackingAsync(ConsolidatedOrderShippedEvent message, CancellationToken cancellationToken)
    {
        var shipment = message.Shipment;

        if (!ConsolidatedOrderShippedRules.IsOrderTrackingEligible(shipment.ConfirmationType, message.IsExport))
        {
            return;
        }

        var packingSlipId = ConsolidatedOrderShippedRules.ResolvePackingSlipId(
            shipment.WarehouseCode, message.ParentOrderId, shipment.PackingSlipId);
        var orderStatus = ConsolidatedOrderShippedRules.ResolveOrderStatus(shipment.ConfirmationType, message.IsExport);

        var groups = shipment.ShipmentLines
            .Where(line => line.Quantity > 0)
            .GroupBy(line => ConsolidatedOrderShippedRules.ResolveGroupKey(shipment.WarehouseCode, line));

        foreach (var group in groups)
        {
            var lines = group
                .Select(line => new OrderTrackingRelayLine(
                    ItemCode: line.ProductId,
                    CountryOfOrigin: line.CountryOfOrigin ?? "UNKNOWN",
                    HallMarkType: line.Hallmarking ?? "NON",
                    Qty: line.Quantity,
                    ShipmentLineNumber: line.LotId))
                .ToArray();

            var trackingRequest = new OrderTrackingRelayRequest(
                ReferenceId: message.ParentOrderId,
                Channel: message.Channel.ToString(),
                FulfilmentUnitId: shipment.WarehouseCode,
                FulfilmentUnitType: InventoryStateChanged.InventoryEventLocationType.Warehouse.ToString(),
                FunctionName: nameof(ConsolidatedOrderShippedHandler),
                OrderId: group.Key,
                OrderStatus: orderStatus,
                OrderType: OrderType.TRANSFER.ToString(),
                Lines: lines,
                Type: "B2B_CONSOLIDATED_ORDER_SHIPPED",
                PackingSlipId: packingSlipId,
                ShipmentId: shipment.Id,
                ShipDate: shipment.ShipDate,
                Market: message.Market,
                IsExport: message.IsExport);

            await orderTrackingPublisher.PublishAsync(trackingRequest, cancellationToken);
        }
    }
}
