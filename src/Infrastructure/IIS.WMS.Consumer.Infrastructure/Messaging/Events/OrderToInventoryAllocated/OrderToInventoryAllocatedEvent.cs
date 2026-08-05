using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderToInventoryAllocated;

/// <summary>
/// This consumer's own decoupled wire contract for an `OrderToInventoryAllocated` event -
/// mirrors `net.pandora.nexus.event.inventory.OrderToInventoryAllocated` (the Avro-generated
/// SpecificRecord from the NexusFacades.Common.AvroSchemas package) field-for-field, but as a plain
/// type with no Avro codegen ties (no Schema property, no ISpecificRecord) - a future Avro schema
/// change only ripples into `Mappers.OrderToInventoryAllocatedEventMapper`, not into the JSON audit
/// trail/Service Bus payload shape this type defines. Mapped from the Avro type by
/// <see cref="Mappers.OrderToInventoryAllocatedEventMapper"/> (hand-written, no mapping library).
/// </summary>
public sealed record OrderToInventoryAllocatedEvent(
    InventoryEventChannel Channel,
    DateTime AllocateDate,
    InventoryEventLocation Location,
    string? Entity,
    InventoryDomainType OrderDomain,
    string OrderId,
    string ReferenceId,
    string ProductId,
    string ProductUnits,
    string CountryOfOrigin,
    string Hallmarking,
    int AllocatedFromB2BBucketQuantity,
    int AllocatedFromB2CBucketQuantity);

/// <summary>Wire-level enum for order domain, mirroring Avro symbols.</summary>
public enum InventoryDomainType
{
    Unknown = 0,
    B2B = 1,
    B2C = 2,
    InternalHallmarking = 3,
    ExternalHallmarking = 4,
    Omni = 5,
}
