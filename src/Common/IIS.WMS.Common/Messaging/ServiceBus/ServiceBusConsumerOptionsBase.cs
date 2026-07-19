namespace IIS.WMS.Common.Messaging.ServiceBus;

/// <summary>
/// Shared Service Bus session-processor knobs, common to every consumer options type regardless of
/// level - the top-level <see cref="ServiceBusConsumerOptions"/> (<c>ServiceBus</c> section) and every
/// queue-level options type (e.g. <c>InventoryStateChangedServiceBusConsumerOptions</c>, bound from
/// <c>ServiceBus:InventoryStateChanged</c>). A future session-processor setting that needs the same
/// queue-level-first, ServiceBus-level-fallback treatment belongs here once, rather than being
/// duplicated in every derived options type.
/// </summary>
public abstract class ServiceBusConsumerOptionsBase
{
    /// <summary>
    /// Independent aggregates (sessions) a <see cref="Azure.Messaging.ServiceBus.ServiceBusSessionProcessor"/>
    /// processes in parallel (<see cref="Azure.Messaging.ServiceBus.ServiceBusSessionProcessorOptions.MaxConcurrentSessions"/>).
    /// </summary>
    public int? MaxConcurrentSessions { get; set; }

    /// <summary>
    /// Messages processed at a time <em>within</em> a single session
    /// (<see cref="Azure.Messaging.ServiceBus.ServiceBusSessionProcessorOptions.MaxConcurrentCallsPerSession"/>) -
    /// this is what orders processing for a given aggregate, so raising it above 1 trades that
    /// ordering guarantee for throughput.
    /// </summary>
    public int? MaxConcurrentCallsPerSession { get; set; }
}
