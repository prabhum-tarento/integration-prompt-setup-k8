using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged.Mappers;

/// <summary>
/// Hand-written mapping from the Avro-generated
/// <see cref="net.pandora.nexus.@event.inventory.InternalHallmarkingStatusChanged"/> SpecificRecord
/// (NexusFacades.Common.AvroSchemas) to this consumer's own decoupled
/// <see cref="InternalHallmarkingStatusChangedEvent"/> wire contract - no mapping library, same
/// rationale as <see cref="InventoryStateChangedEventMapper"/>. Every Avro type referenced below is
/// fully qualified rather than <c>using</c>'d: this schema's own top-level
/// <c>net.pandora.nexus.event.inventory.Status</c> (STARTED/PICKED/CHANGED/FINISHED) is a DIFFERENT
/// type than <c>net.pandora.nexus.object.inventory.Status</c> (PICKABLE/HELD/PREPARED/HALLMARKING/
/// ALLOCATED/INVOICED, used for <c>inventoryState.status</c>) - an unqualified <c>using</c> for either
/// would risk silently binding <c>Status</c> to the wrong one. <see cref="InventoryStateChangedEventMapper.ToChannel"/>/
/// <see cref="InventoryStateChangedEventMapper.ToLocation"/>/<see cref="InventoryStateChangedEventMapper.ToStateSnapshot"/>
/// are reused as-is (marked <see langword="internal"/> specifically for this kind of cross-mapper
/// reuse) since <c>channel</c>/<c>location</c>/<c>inventoryState</c> are the identical Avro shared
/// shapes already consumed by <c>InventoryStateChanged</c>.
/// </summary>
internal static class InternalHallmarkingStatusChangedEventMapper
{
    public static InternalHallmarkingStatusChangedEvent ToInternalHallmarkingStatusChangedEvent(
        this net.pandora.nexus.@event.inventory.InternalHallmarkingStatusChanged source) =>
        new(
            InventoryStateChangedEventMapper.ToChannel(source.channel),
            ToStatus(source.status),
            source.id,
            source.changeDate,
            InventoryStateChangedEventMapper.ToLocation(source.location),
            source.entity,
            InventoryStateChangedEventMapper.ToChangeType(source.type),
            InventoryStateChangedEventMapper.ToStateSnapshot(source.inventoryState),
            ToItemLine(source.itemLine));

    private static HallmarkingItemLine ToItemLine(net.pandora.nexus.@object.inventory.HallmarkingItemLine line) =>
        new(
            line.lineNum,
            line.productId,
            line.quantity,
            line.countryOfOrigin.ToString(),
            line.hallmarkingFrom,
            line.hallmarkingTo,
            line.reasonCode);

    private static Status ToStatus(net.pandora.nexus.@event.inventory.Status status) => status switch
    {
        net.pandora.nexus.@event.inventory.Status.STARTED => Status.Started,
        net.pandora.nexus.@event.inventory.Status.PICKED => Status.Picked,
        net.pandora.nexus.@event.inventory.Status.CHANGED => Status.Changed,
        net.pandora.nexus.@event.inventory.Status.FINISHED => Status.Finished,
        _ => Status.Unknown,
    };
}
