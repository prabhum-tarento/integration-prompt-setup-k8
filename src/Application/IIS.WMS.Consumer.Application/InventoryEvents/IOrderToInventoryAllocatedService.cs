using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Business logic for applying an order-to-inventory-allocation event to the inventory aggregate
/// (docs/events/inventory.OrderToInventoryAllocated.md §3.2–§3.8). Encapsulates the Cosmos
/// read-retry-on-412-write loop and delegates downstream notifications to specialized publishers.
/// </summary>
public interface IOrderToInventoryAllocatedService
{
    /// <summary>
    /// Allocates inventory from B2B and/or B2C buckets to an order, with optional B2C extension
    /// recalculation and item-level segmentation.
    /// </summary>
    /// <param name="fulfilmentId">Fulfilment location the allocation targets.</param>
    /// <param name="itemCode">Item code being allocated.</param>
    /// <param name="countryOfOrigin">Country of origin of the item.</param>
    /// <param name="hallmark">Hallmarking value of the item.</param>
    /// <param name="orderDomain">Business domain classification (B2B/B2C/InternalHallmarking/ExternalHallmarking).</param>
    /// <param name="allocatedFromB2BBucketQuantity">Quantity to allocate from B2B bucket.</param>
    /// <param name="allocatedFromB2CBucketQuantity">Quantity to allocate from B2C bucket.</param>
    /// <param name="isThirdPartyLogistics">Whether the fulfilment location is CAECOM (3PL), drives location-type in downstream publishes.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>OMS delta metrics (zero values if no change).</returns>
    Task<ItemStockInventoryDeltaResult> AllocateAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        OrderDomain orderDomain, int allocatedFromB2BBucketQuantity, int allocatedFromB2CBucketQuantity,
        bool isThirdPartyLogistics, CancellationToken cancellationToken = default);
}
