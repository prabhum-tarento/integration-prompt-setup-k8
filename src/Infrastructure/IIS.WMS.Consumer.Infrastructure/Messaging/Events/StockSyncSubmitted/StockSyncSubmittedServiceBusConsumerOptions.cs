using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockSyncSubmitted;

/// <summary>
/// Bound from the <c>ServiceBus:StockSyncSubmitted</c> configuration section -
/// <see cref="StockSyncSubmittedServiceBusHostedService"/>'s queue-specific settings
/// (docs/events/inventory.StockSyncSubmitted.md §9). Owns its own <see cref="QueueName"/>, following
/// <see cref="InventoryAdjusted.InventoryAdjustedServiceBusConsumerOptions"/>'s precedent - relayed
/// onto its own dedicated <c>stock-sync-submitted</c> queue.
/// </summary>
public sealed class StockSyncSubmittedServiceBusConsumerOptions : ServiceBusConsumerOptionsBase
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "ServiceBus:StockSyncSubmitted";

    /// <summary>Name of the session-enabled queue this consumer processes.</summary>
    public string QueueName { get; init; } = "stock-sync-submitted";

    /// <summary>Bounded fan-out degree for per-group processing within one message (see <see cref="Handlers.StockSyncSubmittedHandler"/>).</summary>
    public int MaxItemLineParallelism { get; init; } = 8;

    /// <summary>Fills in <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentSessions"/>/<see cref="ServiceBusConsumerOptionsBase.MaxConcurrentCallsPerSession"/> from the top-level <c>ServiceBus</c> section when left unset here.</summary>
    public void ApplyServiceBusLevelDefaults(ServiceBusConsumerOptions serviceBusLevelOptions)
    {
        MaxConcurrentSessions ??= serviceBusLevelOptions.MaxConcurrentSessions;
        MaxConcurrentCallsPerSession ??= serviceBusLevelOptions.MaxConcurrentCallsPerSession;
    }
}
