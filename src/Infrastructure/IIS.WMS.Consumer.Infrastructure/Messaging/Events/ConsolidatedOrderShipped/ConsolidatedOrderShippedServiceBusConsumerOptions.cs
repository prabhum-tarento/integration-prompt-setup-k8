using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped;

/// <summary>
/// Bound from the <c>ServiceBus:ConsolidatedOrderShipped</c> configuration section - queue-level
/// session-processor overrides for <see cref="ConsolidatedOrderShippedServiceBusHostedService"/>. Leave
/// <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentSessions"/>/<see cref="ServiceBusConsumerOptionsBase.MaxConcurrentCallsPerSession"/>
/// unset for the common case of this queue sharing the top-level <c>ServiceBus</c> section's values via
/// <see cref="ApplyServiceBusLevelDefaults"/>; set them here only when this queue specifically needs a
/// different concurrency profile, mirroring
/// <see cref="OrderStatusChanged.OrderStatusChangedServiceBusConsumerOptions"/>.
/// </summary>
public sealed class ConsolidatedOrderShippedServiceBusConsumerOptions : ServiceBusConsumerOptionsBase
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "ServiceBus:ConsolidatedOrderShipped";

    /// <summary>
    /// Queue this consumer reads from - its own dedicated queue (docs/events/b2b.sales.ConsolidatedOrderShipped.md
    /// §7/§9), not the top-level <see cref="ServiceBusConsumerOptions.QueueName"/>.
    /// </summary>
    public string QueueName { get; init; } = "consolidated-order-shipped";

    /// <summary>
    /// Fills <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentSessions"/> and
    /// <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentCallsPerSession"/> from
    /// <paramref name="serviceBusLevelOptions"/> wherever this (queue-level) instance left them unset -
    /// queue level wins whenever it's configured, ServiceBus level is only the fallback.
    /// </summary>
    /// <param name="serviceBusLevelOptions">The resolved top-level <c>ServiceBus</c> section options this queue's unset settings fall back to.</param>
    public void ApplyServiceBusLevelDefaults(ServiceBusConsumerOptions serviceBusLevelOptions)
    {
        MaxConcurrentSessions ??= serviceBusLevelOptions.MaxConcurrentSessions;
        MaxConcurrentCallsPerSession ??= serviceBusLevelOptions.MaxConcurrentCallsPerSession;
    }
}
