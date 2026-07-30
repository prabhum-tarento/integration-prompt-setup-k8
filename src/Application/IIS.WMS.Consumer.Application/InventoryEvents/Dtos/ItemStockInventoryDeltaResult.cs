namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>
/// Output metrics for B2C extension calculations - tracks whether the B2C available quantity
/// changed and the delta amount for OMS relay purposes.
/// </summary>
public sealed class ItemStockInventoryDeltaResult
{
    /// <summary>
    /// Whether the B2C available quantity changed after the pick/unpick and extension recalculation.
    /// </summary>
    public bool IsB2CChanged { get; set; }

    /// <summary>
    /// The amount by which B2C available changed (may be negative for decreases).
    /// Zero if no change occurred or the record was not extended.
    /// </summary>
    public int DeltaTowardsOms { get; set; }
}
