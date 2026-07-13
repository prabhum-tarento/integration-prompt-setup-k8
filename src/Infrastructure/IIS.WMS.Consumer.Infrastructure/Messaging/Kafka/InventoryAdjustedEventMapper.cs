using IIS.WMS.Consumer.Infrastructure.Messaging;
using net.pandora.nexus.@event.inventory;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Hand-written mapping from the Avro-generated <see cref="InventoryAdjusted"/> SpecificRecord
/// (NexusFacades.Common.AvroSchemas) to this consumer's own decoupled
/// <see cref="InventoryAdjustedEvent"/> wire contract - no mapping library, same rationale as
/// <see cref="InventoryStateChangedEventMapper"/>. <c>state</c>, <c>location</c>, <c>type</c>, and each
/// <c>adjustmentLines</c> entry share the exact same Avro shapes <see cref="InventoryStateChangedEventMapper"/>
/// already maps (<c>net.pandora.nexus.object.inventory.InventoryState</c>/<c>InventoryChangeType</c>/
/// <c>ItemLine</c>, <c>net.pandora.nexus.shared.Location</c>), so this reuses that mapper's private helpers
/// rather than duplicating them. Every enum is mapped explicitly by symbol name, not by ordinal - same
/// reasoning as <see cref="InventoryStateChangedEventMapper"/>.
/// </summary>
internal static class InventoryAdjustedEventMapper
{
    public static InventoryAdjustedEvent ToInventoryAdjustedEvent(this InventoryAdjusted source) =>
        new(
            InventoryStateChangedEventMapper.ToChannel(source.channel),
            ToAdjustment(source.adjustment));

    private static InventoryEventAdjustment ToAdjustment(Adjustment adjustment) =>
        new(
            adjustment.referenceId,
            adjustment.adjustmentDate,
            adjustment.entity,
            InventoryStateChangedEventMapper.ToChangeType(adjustment.type),
            InventoryStateChangedEventMapper.ToStateSnapshot(adjustment.state),
            InventoryStateChangedEventMapper.ToLocation(adjustment.location),
            ToReasonCode(adjustment.reason),
            adjustment.adjustmentLines.Select(InventoryStateChangedEventMapper.ToItemLine).ToArray());

    private static InventoryEventReasonCode ToReasonCode(net.pandora.nexus.@object.inventory.ReasonCode reason) => reason switch
    {
        net.pandora.nexus.@object.inventory.ReasonCode.ADJUSTMENT => InventoryEventReasonCode.Adjustment,
        net.pandora.nexus.@object.inventory.ReasonCode.BUNDLING => InventoryEventReasonCode.Bundling,
        net.pandora.nexus.@object.inventory.ReasonCode.COUNTING => InventoryEventReasonCode.Counting,
        net.pandora.nexus.@object.inventory.ReasonCode.CUSTOMER_RETURN => InventoryEventReasonCode.CustomerReturn,
        net.pandora.nexus.@object.inventory.ReasonCode.OTHER => InventoryEventReasonCode.Other,
        net.pandora.nexus.@object.inventory.ReasonCode.RECEIPT => InventoryEventReasonCode.Receipt,
        net.pandora.nexus.@object.inventory.ReasonCode.RECEIPT_ADJUSTMENT => InventoryEventReasonCode.ReceiptAdjustment,
        net.pandora.nexus.@object.inventory.ReasonCode.RETURN => InventoryEventReasonCode.Return,
        net.pandora.nexus.@object.inventory.ReasonCode.SALE => InventoryEventReasonCode.Sale,
        net.pandora.nexus.@object.inventory.ReasonCode.TRANSFER => InventoryEventReasonCode.Transfer,
        net.pandora.nexus.@object.inventory.ReasonCode.VENDOR_RETURN => InventoryEventReasonCode.VendorReturn,
        net.pandora.nexus.@object.inventory.ReasonCode.AUTO_RECONCILIATION => InventoryEventReasonCode.AutoReconciliation,
        _ => InventoryEventReasonCode.Unknown,
    };
}
