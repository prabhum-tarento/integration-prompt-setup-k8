using System.Text.Json;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.OrderTracking;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived.Handlers;

/// <summary>
/// Applies one relayed <see cref="GoodsInTransitReceivedEvent"/> (docs/events/b2b.purchase.GoodsInTransitReceived.md
/// §2-§3). Per shipment line (processed sequentially, doc §10 known limitation): resolves the packing-slip
/// id, sellability, state/status, fulfilment-unit-id, and destination-node via
/// <see cref="GoodsInTransitReceivedRules"/> (shared with
/// <see cref="GoodsInTransitReceivedConsumerHostedService"/>'s own Kafka-side routing so both layers agree),
/// applies the §3.7 inventory update, and publishes the §3.6 OMS delta when eligible. Publishes exactly one
/// order-tracking request per message (§6/§7.3 Output 1) after every line has been applied.
/// </summary>
/// <param name="goodsInTransitReceivedService">§3.7 sellable/non-sellable inventory update logic.</param>
/// <param name="deltaTowardsOmsPublisher">§3.6/§7.3 Output 2 OMS delta publisher (feature-flag gated).</param>
/// <param name="orderTrackingPublisher">§7.3 Output 1 order-tracking publisher (always attempted, best-effort).</param>
/// <param name="archiveWriter">Message archive background writer (best-effort).</param>
/// <param name="featureFlagsOptions">Feature flag options for the OMS delta publish gate.</param>
/// <param name="logger">Logger for per-line and overall processing events.</param>
public sealed class GoodsInTransitReceivedHandler(
    IGoodsInTransitReceivedService goodsInTransitReceivedService,
    IDeltaTowardsOmsPublisher deltaTowardsOmsPublisher,
    IOrderTrackingPublisher orderTrackingPublisher,
    IMessageArchiveWriter archiveWriter,
    IOptions<FeatureFlagsOptions> featureFlagsOptions,
    ILogger<GoodsInTransitReceivedHandler> logger) : IGoodsInTransitReceivedHandler
{
    private const string EventTypeName = "b2b.purchase.GoodsInTransitReceived";
    private const string SapWarehouseCode = "TDC-SAP-ID";

    /// <inheritdoc/>
    public async Task HandleAsync(GoodsInTransitReceivedEvent message, string correlationId, CancellationToken cancellationToken)
    {
        var shipment = message.Shipment;
        var packingSlipId = GoodsInTransitReceivedRules.NormalizePackingSlipId(shipment.PackingSlipId);

        // Archive BEFORE (best-effort, non-blocking)
        archiveWriter.Enqueue(Domain.Aggregates.MessageArchive.Create(
            $"{EventTypeName}:{packingSlipId}",
            EventTypeName,
            JsonSerializer.Serialize(message),
            correlationId,
            DateTime.UtcNow));

        var isSellable = GoodsInTransitReceivedRules.IsSellable(shipment.LocationTo);
        var isOmsDeltaEligible = GoodsInTransitReceivedRules.IsOmsDeltaEligible(shipment.WarehouseCode, shipment.LocationTo);
        var fulfilmentUnitId = GoodsInTransitReceivedRules.ResolveFulfilmentUnitId(shipment.LocationTo, shipment.WarehouseCode, shipment.VendorCode);
        var destinationNode = GoodsInTransitReceivedRules.ResolveDestinationNode(shipment.LocationTo, shipment.WarehouseCode);

        var trackingLines = new List<OrderTrackingRelayLine>();

        foreach (var line in shipment.ShipmentLines)
        {
            var state = GoodsInTransitReceivedRules.ResolveState(line.ReturnReasonCode);
            var status = GoodsInTransitReceivedRules.ResolveStatus();
            var countryOfOrigin = line.CountryOfOrigin ?? "UNKNOWN";
            var hallmarking = line.Hallmarking ?? "NON";

            var result = await goodsInTransitReceivedService.ReceiveShipmentLineAsync(
                destinationNode,
                line.ProductId,
                countryOfOrigin,
                hallmarking,
                line.Quantity,
                isSellable,
                state,
                status,
                cancellationToken);

            logger.LogInformation(
                "GOODS_IN_TRANSIT_RECEIVED_COMPLETED: PackingSlipId={PackingSlipId}, ProductId={ProductId}, Quantity={Quantity}, Sellable={IsSellable}, CorrelationId={CorrelationId}.",
                packingSlipId, line.ProductId, line.Quantity, isSellable, correlationId);

            // §3.6/§7.3 Output 2: OMS delta - fixed (AVAILABLE, PICKABLE) pair regardless of this line's own state/status
            if (isOmsDeltaEligible && result.IsB2CChanged && featureFlagsOptions.Value.EnableDeltaTowardsOms)
            {
                await deltaTowardsOmsPublisher.PublishAsync(
                    line.ProductId,
                    destinationNode,
                    InventoryStateChanged.InventoryEventLocationType.Warehouse.ToString(),
                    countryOfOrigin,
                    hallmarking,
                    result.DeltaTowardsOms,
                    packingSlipId,
                    cancellationToken);
            }

            trackingLines.Add(new OrderTrackingRelayLine(
                ItemCode: line.ProductId,
                CountryOfOrigin: countryOfOrigin,
                HallMarkType: hallmarking,
                Qty: line.Quantity));
        }

        // §7.3 Output 1: order tracking - published once per message, after every line is applied
        // TODO(ai): unresolved precedence conflict - doc §7.3 OrderTrackingCommonRequest has more fields
        // (SourceNode, ShipmentId, PackingSlipId, Source, Type, ReceivedDate, CustomerId, DestinationNode)
        // than the established OrderTrackingRelayRequest DTO; reusing the established DTO per CLAUDE.md
        // precedence (more specific/existing shape wins), extra fields dropped.
        var trackingRequest = new OrderTrackingRelayRequest(
            ReferenceId: packingSlipId,
            Channel: message.Channel.ToString(),
            FulfilmentUnitId: fulfilmentUnitId,
            FulfilmentUnitType: InventoryStateChanged.InventoryEventLocationType.Warehouse.ToString(),
            FunctionName: nameof(GoodsInTransitReceivedHandler),
            OrderId: packingSlipId,
            OrderStatus: OrderTrackingStatus.RECEIVED,
            OrderType: OrderType.TRANSFER.ToString(),
            Lines: trackingLines);

        await orderTrackingPublisher.PublishAsync(trackingRequest, cancellationToken);

        // Archive AFTER (best-effort, non-blocking)
        archiveWriter.Enqueue(Domain.Aggregates.MessageArchive.Create(
            $"{EventTypeName}:after:{packingSlipId}",
            $"{EventTypeName}:after",
            JsonSerializer.Serialize(new { packingSlipId, fulfilmentUnitId, destinationNode }),
            correlationId,
            DateTime.UtcNow));
    }
}
