namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>
/// Output metrics for one §3 goods-in-transit-received shipment line
/// (docs/events/b2b.purchase.GoodsInTransitReceived.md) - tracks whether the sellable B2C available
/// quantity changed and the delta amount for OMS relay purposes. Mirrors <see cref="ItemStockInventoryDeltaResult"/>.
/// </summary>
public sealed class GoodsInTransitReceiptResult
{
    /// <summary>Whether the sellable record's B2C available quantity changed. Always <see langword="false"/> for the non-sellable path.</summary>
    public bool IsB2CChanged { get; set; }

    /// <summary>The amount by which B2C available changed. Zero if the line was routed to the non-sellable path or no change occurred.</summary>
    public int DeltaTowardsOms { get; set; }
}
