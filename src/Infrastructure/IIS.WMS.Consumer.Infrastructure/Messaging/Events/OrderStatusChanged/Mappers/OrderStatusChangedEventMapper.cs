using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged.Mappers;

/// <summary>
/// Hand-written mapping from the Avro-generated
/// <see cref="net.pandora.nexus.@event.b2b.sales.OrderStatusChanged"/> SpecificRecord
/// (NexusFacades.Common.AvroSchemas) to this consumer's own decoupled <see cref="OrderStatusChangedEvent"/>
/// wire contract - no mapping library, same rationale as <see cref="InventoryStateChangedEventMapper"/>.
/// Every Avro type referenced below is fully qualified rather than <c>using</c>'d.
/// <see cref="InventoryStateChangedEventMapper.ToChannel"/> is reused as-is since <c>channel</c> is the
/// identical Avro shared shape already consumed by <c>InventoryStateChanged</c>.
/// </summary>
internal static class OrderStatusChangedEventMapper
{
    public static OrderStatusChangedEvent ToOrderStatusChangedEvent(
        this net.pandora.nexus.@event.b2b.sales.OrderStatusChanged source) =>
        new(
            InventoryStateChangedEventMapper.ToChannel(source.channel),
            source.market?.ToString(),
            source.sellingLegalEntity,
            source.orderId,
            source.backOrderId,
            source.pickingRouteId,
            ToStatus(source.status),
            source.warehouseCode,
            source.isReturn,
            source.changeDate,
            source.cancelReason,
            source.sourceOrderReferenceId);

    private static OrderStatusCode ToStatus(net.pandora.nexus.@event.b2b.sales.StatusCode status) => status switch
    {
        net.pandora.nexus.@event.b2b.sales.StatusCode.DEACTIVATED => OrderStatusCode.Deactivated,
        net.pandora.nexus.@event.b2b.sales.StatusCode.NOT_RUN => OrderStatusCode.NotRun,
        net.pandora.nexus.@event.b2b.sales.StatusCode.RUN => OrderStatusCode.Run,
        net.pandora.nexus.@event.b2b.sales.StatusCode.COLLECTION_STARTED => OrderStatusCode.CollectionStarted,
        net.pandora.nexus.@event.b2b.sales.StatusCode.COLLECTION_PERFORMED => OrderStatusCode.CollectionPerformed,
        net.pandora.nexus.@event.b2b.sales.StatusCode.PREPARATION_IN_PROGRESS => OrderStatusCode.PreparationInProgress,
        net.pandora.nexus.@event.b2b.sales.StatusCode.TO_PACKAGE => OrderStatusCode.ToPackage,
        net.pandora.nexus.@event.b2b.sales.StatusCode.COMPLETED => OrderStatusCode.Completed,
        net.pandora.nexus.@event.b2b.sales.StatusCode.DESPATCHED => OrderStatusCode.Despatched,
        net.pandora.nexus.@event.b2b.sales.StatusCode.CANCELLED => OrderStatusCode.Cancelled,
        net.pandora.nexus.@event.b2b.sales.StatusCode.DELETED => OrderStatusCode.Deleted,
        net.pandora.nexus.@event.b2b.sales.StatusCode.ORDER_CANCELED => OrderStatusCode.OrderCanceled,
        net.pandora.nexus.@event.b2b.sales.StatusCode.CREDIT_BLOCKED => OrderStatusCode.CreditBlocked,
        net.pandora.nexus.@event.b2b.sales.StatusCode.CREDIT_UNBLOCKED => OrderStatusCode.CreditUnblocked,
        _ => OrderStatusCode.Unknown,
    };
}
