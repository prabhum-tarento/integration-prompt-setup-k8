namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// Minimal item-master existence record (docs/events/inventory.StockSyncSubmitted.md assumption 3;
/// corroborated by docs/events/inventory.StockOnHandUpdated.md's own "product/item existence
/// validation and creation" treatment) - tracks only whether an item code is known to IIS master data.
/// Missing items are auto-created by the caller (not this type) rather than rejected.
/// </summary>
public class Item
{
    public string ItemCode { get; set; } = default!;
}
