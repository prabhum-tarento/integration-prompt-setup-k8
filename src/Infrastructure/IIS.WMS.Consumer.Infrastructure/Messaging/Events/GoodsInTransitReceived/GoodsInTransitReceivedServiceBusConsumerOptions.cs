using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived;

/// <summary>
/// Bound from the <c>ServiceBus:GoodsInTransitReceived</c> configuration section - queue-level
/// session-processor overrides for <see cref="GoodsInTransitReceivedServiceBusHostedService"/>. Leave
/// <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentSessions"/>/<see cref="ServiceBusConsumerOptionsBase.MaxConcurrentCallsPerSession"/>
/// unset for the common case of this queue sharing the top-level <c>ServiceBus</c> section's values via
/// <see cref="ApplyServiceBusLevelDefaults"/>; set them here only when this queue specifically needs a
/// different concurrency profile.
/// </summary>
public sealed class GoodsInTransitReceivedServiceBusConsumerOptions : ServiceBusConsumerOptionsBase
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "ServiceBus:GoodsInTransitReceived";

    /// <summary>
    /// Queue this consumer reads from - its own dedicated queue (docs/events/b2b.purchase.GoodsInTransitReceived.md
    /// §7.2), not the top-level <see cref="ServiceBusConsumerOptions.QueueName"/>, mirroring
    /// <see cref="OrderStatusChanged.OrderStatusChangedServiceBusConsumerOptions.QueueName"/>'s own-queue pattern.
    /// </summary>
    public string QueueName { get; init; } = "goods-in-transit-received";

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
