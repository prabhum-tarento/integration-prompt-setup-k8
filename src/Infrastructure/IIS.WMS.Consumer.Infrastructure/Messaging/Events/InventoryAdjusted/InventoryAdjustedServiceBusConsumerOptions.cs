using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryAdjusted;

/// <summary>
/// Bound from the <c>ServiceBus:InventoryAdjusted</c> configuration section -
/// <see cref="InventoryAdjustedServiceBusHostedService"/>'s queue-specific settings
/// (docs/events/inventory.InventoryAdjusted.md §9). Unlike
/// <see cref="InventoryStateChanged.InventoryStateChangedServiceBusConsumerOptions"/>, this type owns
/// its own <see cref="QueueName"/> rather than falling back to the top-level
/// <see cref="ServiceBusConsumerOptions.QueueName"/> - <c>InventoryAdjusted</c> is relayed onto its own
/// dedicated <c>inventory-adjusted</c> queue, not the shared one <c>InventoryStateChanged</c> uses.
/// Follows <see cref="BulkImportServiceBusConsumerOptions"/>'s precedent for a queue-specific
/// <c>QueueName</c> rather than <c>InventoryStateChangedServiceBusConsumerOptions</c>'s.
/// </summary>
public sealed class InventoryAdjustedServiceBusConsumerOptions : ServiceBusConsumerOptionsBase
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "ServiceBus:InventoryAdjusted";

    /// <summary>Name of the session-enabled queue this consumer processes.</summary>
    public string QueueName { get; init; } = "inventory-adjusted";

    /// <summary>Bounded fan-out degree for per-adjustment-line processing within one message (see <see cref="Handlers.InventoryAdjustedHandler"/>).</summary>
    public int MaxItemLineParallelism { get; init; } = 8;

    /// <summary>Fills in <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentSessions"/>/<see cref="ServiceBusConsumerOptionsBase.MaxConcurrentCallsPerSession"/> from the top-level <c>ServiceBus</c> section when left unset here.</summary>
    public void ApplyServiceBusLevelDefaults(ServiceBusConsumerOptions serviceBusLevelOptions)
    {
        MaxConcurrentSessions ??= serviceBusLevelOptions.MaxConcurrentSessions;
        MaxConcurrentCallsPerSession ??= serviceBusLevelOptions.MaxConcurrentCallsPerSession;
    }
}
