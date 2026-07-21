namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

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
}
