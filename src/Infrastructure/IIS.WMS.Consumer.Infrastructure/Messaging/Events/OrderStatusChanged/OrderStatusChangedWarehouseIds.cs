namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged;

/// <summary>
/// Warehouse identifiers referenced by <see cref="Handlers.OrderStatusChangedHandler"/>'s warehouse
/// classification/normalization (docs/events/b2b.sales.OrderStatusChanged.md §3.1/§3.4/§4/§9) - scoped
/// to just this event, mirroring <see cref="InventoryStateChanged.FulfilmentLocationIds"/>'s own
/// local-constants-class precedent (no shared <c>ReflexConstants</c> class exists in this codebase).
/// </summary>
internal static class OrderStatusChangedWarehouseIds
{
    /// <summary>TDC's SAP id - normalizes to <see cref="Tdc"/> on the output request (§3.4).</summary>
    public const string TdcSapId = "D001";

    public const string Tdc = "TDC";

    public const string Adc = "ADC";

    /// <summary>Case-insensitive membership check for §3.1's TDC/ADC classification - O(1) and casing-tolerant per §4.</summary>
    public static readonly HashSet<string> SpecialWarehouses = new(StringComparer.OrdinalIgnoreCase)
    {
        TdcSapId,
        Tdc,
        Adc,
    };
}
