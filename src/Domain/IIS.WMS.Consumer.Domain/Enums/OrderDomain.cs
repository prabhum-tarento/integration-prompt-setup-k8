namespace IIS.WMS.Consumer.Domain.Enums;

/// <summary>
/// Order business domain classification - used to determine which inventory bucket
/// (B2B/B2C/Internal Hallmarking/External Hallmarking) an allocation targets, mirroring
/// the Avro <c>net.pandora.nexus.object.inventory.InventoryDomain</c> enum but decoupled
/// from codegen (docs/events/inventory.OrderToInventoryAllocated.md §3.2).
/// </summary>
public enum OrderDomain
{
    /// <summary>Unknown or unrecognized domain.</summary>
    Unknown = 0,

    /// <summary>B2B order domain - allocates from B2BAllocated.</summary>
    B2B = 1,

    /// <summary>B2C order domain - allocates from B2CAllocated; may draw from B2BUsedShare if extended.</summary>
    B2C = 2,

    /// <summary>Internal hallmarking domain - allocates from B2BAllocated for hallmark-in-process inventory.</summary>
    InternalHallmarking = 3,

    /// <summary>External hallmarking domain - allocates from B2BAllocated for customer-supplied hallmark-in-process inventory.</summary>
    ExternalHallmarking = 4,

    /// <summary>Omni channel domain.</summary>
    Omni = 5,
}
