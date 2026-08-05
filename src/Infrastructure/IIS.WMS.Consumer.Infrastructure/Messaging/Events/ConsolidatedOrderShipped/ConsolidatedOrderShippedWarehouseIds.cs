namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped;

/// <summary>
/// Warehouse identifiers referenced by <see cref="ConsolidatedOrderShippedRules"/>'s warehouse
/// classification (docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.2/§4) - scoped to just this
/// event, mirroring <see cref="OrderStatusChanged.OrderStatusChangedWarehouseIds"/>'s own
/// local-constants-class precedent.
/// </summary>
internal static class ConsolidatedOrderShippedWarehouseIds
{
    /// <summary>TDC's SAP id - classified alongside <see cref="Tdc"/>/<see cref="Adc"/> for grouping purposes (§3.2).</summary>
    public const string TdcSapId = "D001";

    public const string Tdc = "TDC";

    public const string Adc = "ADC";

    /// <summary>Case-insensitive membership check for §3.2's TDC/ADC vs. 3PL classification.</summary>
    public static readonly HashSet<string> SpecialWarehouses = new(StringComparer.OrdinalIgnoreCase)
    {
        TdcSapId,
        Tdc,
        Adc,
    };
}
