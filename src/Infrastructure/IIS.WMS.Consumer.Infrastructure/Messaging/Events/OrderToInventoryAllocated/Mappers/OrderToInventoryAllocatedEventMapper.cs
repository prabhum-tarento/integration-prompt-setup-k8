using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Mappers;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderToInventoryAllocated.Mappers;

/// <summary>
/// Hand-written mapping from the Avro-generated <see cref="net.pandora.nexus.@event.inventory.OrderToInventoryAllocated"/> SpecificRecord
/// (NexusFacades.Common.AvroSchemas) to this consumer's own decoupled
/// <see cref="OrderToInventoryAllocatedEvent"/> wire contract - no mapping library.
/// Reuses <see cref="InventoryStateChangedEventMapper.ToChannel"/> and
/// <see cref="InventoryStateChangedEventMapper.ToLocation"/> where shapes match.
/// </summary>
internal static class OrderToInventoryAllocatedEventMapper
{
    /// <summary>
    /// Maps from the Avro SpecificRecord to the decoupled DTO.
    /// </summary>
    internal static OrderToInventoryAllocatedEvent ToOrderToInventoryAllocatedEvent(
        this net.pandora.nexus.@event.inventory.OrderToInventoryAllocated source) =>
        new(
            Channel: InventoryStateChangedEventMapper.ToChannel(source.channel),
            AllocateDate: source.allocateDate,
            Location: InventoryStateChangedEventMapper.ToLocation(source.location),
            Entity: source.entity,
            OrderDomain: ToOrderDomain(source.orderDomain),
            OrderId: source.orderId,
            ReferenceId: source.referenceId,
            ProductId: source.productId,
            ProductUnits: source.productUnits,
            CountryOfOrigin: source.countryOfOrigin.ToString(),
            Hallmarking: source.hallmarking,
            AllocatedFromB2BBucketQuantity: source.allocatedFromB2BBucketQuantity,
            AllocatedFromB2CBucketQuantity: source.allocatedFromB2CBucketQuantity);

    private static InventoryDomainType ToOrderDomain(net.pandora.nexus.@object.inventory.InventoryDomain? source) =>
        source switch
        {
            net.pandora.nexus.@object.inventory.InventoryDomain.B2B => InventoryDomainType.B2B,
            net.pandora.nexus.@object.inventory.InventoryDomain.B2C => InventoryDomainType.B2C,
            net.pandora.nexus.@object.inventory.InventoryDomain.INTERNAL_HALLMARKING => InventoryDomainType.InternalHallmarking,
            net.pandora.nexus.@object.inventory.InventoryDomain.EXTERNAL_HALLMARKING => InventoryDomainType.ExternalHallmarking,
            net.pandora.nexus.@object.inventory.InventoryDomain.OMNI => InventoryDomainType.Omni,
            _ => InventoryDomainType.Unknown,
        };
}
