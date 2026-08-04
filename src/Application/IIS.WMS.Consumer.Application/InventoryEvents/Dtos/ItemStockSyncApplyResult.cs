namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>
/// Output of a §3.2 stock-sync Set (docs/events/inventory.StockSyncSubmitted.md) - carries the
/// pre-mutation B2C available quantity so the caller can run the §3.2 discrepancy check
/// (<c>IISAvlQty != avlPickableQnty</c>) against the value the sync just overwrote, without the
/// Application layer itself knowing about <c>ItemDiscrepencyDetail</c> persistence.
/// </summary>
public sealed class ItemStockSyncApplyResult
{
    /// <summary>The B2C available quantity as it stood immediately before this sync was applied - the doc's <c>IISAvlQty</c>.</summary>
    public int PreviousB2CAvailable { get; set; }

    /// <summary>The newly Set (not incremented) B2C available quantity.</summary>
    public int NewB2CAvailable { get; set; }

    /// <summary>Whether no <c>ItemStockInventory</c> record existed yet and one was created rather than patched.</summary>
    public bool WasCreated { get; set; }
}
