using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Business logic for applying one shipment line's B2B confirmation to the inventory aggregate
/// (docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.1, §4.1). Encapsulates the Cosmos
/// read-retry-on-412-write loop; downstream OMS-delta/ICR/order-tracking notifications are the
/// caller's responsibility, mirroring how <see cref="IOrderToInventoryAllocatedService"/> only
/// owns allocation and segmentation.
/// </summary>
public interface IConsolidatedOrderShippedService
{
    /// <summary>
    /// Applies a shipment line's confirmed quantity against B2B/PSC buckets, per the §4.1
    /// confirmation-type arithmetic, with optional B2C extension recalculation when the record is
    /// extended. If no matching <see cref="Domain.Aggregates.ItemStockInventory"/> record exists, or
    /// the request fails §7's validation table (non-positive <see cref="B2BOrderConfirmedRequest.ShippedQuantity"/>,
    /// or <see cref="B2BOrderConfirmedRequest.AllocatedFromB2BBucketQuantity"/> less than
    /// <see cref="B2BOrderConfirmedRequest.ShippedQuantity"/>), logs a warning and returns without
    /// mutating (non-critical bypass) - a tolerated business condition, not a poison message.
    /// </summary>
    /// <param name="request">The shipment line's confirmation details.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>OMS delta metrics (zero values if no change, or the request was bypassed).</returns>
    Task<ItemStockInventoryDeltaResult> ConfirmAsync(
        B2BOrderConfirmedRequest request, CancellationToken cancellationToken = default);
}
