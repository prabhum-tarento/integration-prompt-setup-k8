namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

/// <summary>
/// Fulfilment-center location IDs referenced by <see cref="Validators.InventoryStateChangedEventValidator"/>'s
/// business-rule checks - mirrors the same location IDs the upstream Reflex facade's own
/// <c>ReflexConstants</c> defines (<c>EDCFulfilmentId</c>/<c>TDCFulfilmentId</c>/<c>ADCFulfilmentId</c>),
/// scoped down to just what this consumer's validation needs.
/// </summary>
internal static class FulfilmentLocationIds
{
    public const string Edc = "EDC";
    public const string Tdc = "TDC";
    public const string Adc = "ADC";

    /// <summary>Third-party-logistics fulfilment location - drives the §3.6/§3.8 location-type branches (docs/events/inventory.InventoryStateChanged.md).</summary>
    public const string Caecom = "CAECOM";

    /// <summary>
    /// Brazil 3PL consignee location id as reported on an inbound <c>StockSyncSubmitted</c>
    /// event's <c>Location.Id</c> - mapped internally to <see cref="BrzDc3PlFulfilmentId"/> per
    /// docs/events/inventory.StockSyncSubmitted.md §3.1/§9 assumption 2.
    /// </summary>
    public const string Brz3PlConsigneeId = "BRZ3PLConsigneeId";

    /// <summary>
    /// Internal fulfilment id <see cref="Brz3PlConsigneeId"/> resolves to for BR-market inventory/OMS
    /// processing - must match <c>ItemStockInventorySuffix.Brz3Pl</c>
    /// (Persistence/CosmosDb/Shared/CosmosContainerNames.cs), which Cosmos container resolution parses
    /// this fulfilment code against.
    /// </summary>
    public const string BrzDc3PlFulfilmentId = "BRZ3PL";
}
