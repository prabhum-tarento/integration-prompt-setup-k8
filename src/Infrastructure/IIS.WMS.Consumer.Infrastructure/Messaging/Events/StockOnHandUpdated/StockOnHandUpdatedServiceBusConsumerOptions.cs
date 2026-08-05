using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockOnHandUpdated;

/// <summary>
/// Bound from the <c>ServiceBus:StockOnHandUpdated</c> configuration section -
/// <see cref="StockOnHandUpdatedServiceBusHostedService"/>'s queue-specific settings
/// (docs/events/inventory.StockOnHandUpdated.md §9). Owns its own <see cref="QueueName"/>, following
/// <see cref="StockSyncSubmitted.StockSyncSubmittedServiceBusConsumerOptions"/>'s precedent - relayed
/// onto its own dedicated <c>stock-on-hand-updated</c> queue.
/// </summary>
public sealed class StockOnHandUpdatedServiceBusConsumerOptions : ServiceBusConsumerOptionsBase
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "ServiceBus:StockOnHandUpdated";

    /// <summary>Name of the session-enabled queue this consumer processes.</summary>
    public string QueueName { get; init; } = "stock-on-hand-updated";

    /// <summary>Bounded fan-out degree for per-group processing within one message (see <see cref="Handlers.StockOnHandUpdatedHandler"/>).</summary>
    public int MaxItemLineParallelism { get; init; } = 8;

    /// <summary>Fills in <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentSessions"/>/<see cref="ServiceBusConsumerOptionsBase.MaxConcurrentCallsPerSession"/> from the top-level <c>ServiceBus</c> section when left unset here.</summary>
    public void ApplyServiceBusLevelDefaults(ServiceBusConsumerOptions serviceBusLevelOptions)
    {
        MaxConcurrentSessions ??= serviceBusLevelOptions.MaxConcurrentSessions;
        MaxConcurrentCallsPerSession ??= serviceBusLevelOptions.MaxConcurrentCallsPerSession;
    }
}
