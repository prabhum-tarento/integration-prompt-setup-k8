using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderToInventoryAllocated;

/// <summary>
/// Bound from the <c>ServiceBus:OrderToInventoryAllocated</c> configuration section - settings for the
/// <c>order-to-inventory-allocated</c> Service Bus queue consumer (docs/events/inventory.OrderToInventoryAllocated.md §9).
/// </summary>
public sealed class OrderToInventoryAllocatedServiceBusConsumerOptions : ServiceBusConsumerOptionsBase
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "ServiceBus:OrderToInventoryAllocated";

    /// <summary>Service Bus queue this consumer reads from.</summary>
    public string QueueName { get; init; } = "order-to-inventory-allocated";

    /// <summary>
    /// Applies Service Bus-level defaults for <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentSessions"/>
    /// and <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentCallsPerSession"/>, allowing this queue's
    /// settings to fall back to the top-level <c>ServiceBus</c> section when unset - mirroring the same
    /// event-level-first, Service Bus-level-fallback resolution as Kafka consumers.
    /// </summary>
    public void ApplyServiceBusLevelDefaults(ServiceBusConsumerOptions serviceBusLevelOptions)
    {
        MaxConcurrentSessions ??= serviceBusLevelOptions.MaxConcurrentSessions;
        MaxConcurrentCallsPerSession ??= serviceBusLevelOptions.MaxConcurrentCallsPerSession;
    }
}
