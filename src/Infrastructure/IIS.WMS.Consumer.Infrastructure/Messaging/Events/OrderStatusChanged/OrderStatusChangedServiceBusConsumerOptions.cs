using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged;

/// <summary>
/// Bound from the <c>ServiceBus:OrderStatusChanged</c> configuration section - queue-level
/// session-processor overrides for <see cref="OrderStatusChangedServiceBusHostedService"/>. Leave
/// <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentSessions"/>/<see cref="ServiceBusConsumerOptionsBase.MaxConcurrentCallsPerSession"/>
/// unset for the common case of this queue sharing the top-level <c>ServiceBus</c> section's values via
/// <see cref="ApplyServiceBusLevelDefaults"/>; set them here only when this queue specifically needs a
/// different concurrency profile. This event carries no item lines, so there is no
/// <c>MaxItemLineParallelism</c>-equivalent setting here, same as
/// <see cref="InternalHallmarkingStatusChanged.InternalHallmarkingStatusChangedServiceBusConsumerOptions"/>.
/// </summary>
public sealed class OrderStatusChangedServiceBusConsumerOptions : ServiceBusConsumerOptionsBase
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "ServiceBus:OrderStatusChanged";

    /// <summary>
    /// Queue this consumer reads from - its own dedicated queue (docs/events/b2b.sales.OrderStatusChanged.md
    /// §9), not the top-level <see cref="ServiceBusConsumerOptions.QueueName"/>, mirroring
    /// <see cref="InternalHallmarkingStatusChanged.InternalHallmarkingStatusChangedServiceBusConsumerOptions.QueueName"/>'s
    /// own-queue pattern.
    /// </summary>
    public string QueueName { get; init; } = "order-status-changed";

    /// <summary>
    /// Fills <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentSessions"/> and
    /// <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentCallsPerSession"/> from
    /// <paramref name="serviceBusLevelOptions"/> wherever this (queue-level) instance left them unset -
    /// queue level wins whenever it's configured, ServiceBus level is only the fallback. Called once
    /// from an <see cref="Microsoft.Extensions.Options.IPostConfigureOptions{TOptions}"/> registration
    /// (see <see cref="MessagingServiceCollectionExtensions.RegisterOrderStatusChangedServiceBusConsumer"/>),
    /// after both sections have been bound and after <paramref name="serviceBusLevelOptions"/> has already
    /// had its own defaults applied if unset.
    /// </summary>
    /// <param name="serviceBusLevelOptions">The resolved top-level <c>ServiceBus</c> section options this queue's unset settings fall back to.</param>
    public void ApplyServiceBusLevelDefaults(ServiceBusConsumerOptions serviceBusLevelOptions)
    {
        MaxConcurrentSessions ??= serviceBusLevelOptions.MaxConcurrentSessions;
        MaxConcurrentCallsPerSession ??= serviceBusLevelOptions.MaxConcurrentCallsPerSession;
    }
}
