namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Business logic for the DEECOMDC e-commerce engraving warehouse-stock use case
/// (docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.3 step 3, §5.3).
/// </summary>
public interface IItemStockWarehouseInventoryService
{
    /// <summary>
    /// Increases the engraving warehouse stock for one fulfilment/item combination by
    /// <paramref name="shippedQuantity"/>, creating the record (§5.3 create-if-missing, 409-as-applied)
    /// if it doesn't already exist, or patching an existing record's quantity under an ETag-guarded
    /// retry loop otherwise.
    /// </summary>
    /// <param name="fulfilmentId">Warehouse the shipment left from.</param>
    /// <param name="itemCode">Item code being shipped.</param>
    /// <param name="shippedQuantity">Quantity shipped for this line - must be greater than zero.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task ApplyShipmentAsync(
        string fulfilmentId, string itemCode, int shippedQuantity, CancellationToken cancellationToken = default);
}
