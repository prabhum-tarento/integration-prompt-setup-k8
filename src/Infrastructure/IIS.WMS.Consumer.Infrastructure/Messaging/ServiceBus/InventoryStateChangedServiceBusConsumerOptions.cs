using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;

/// <summary>
/// Bound from the <c>ServiceBus:InventoryStateChanged</c> configuration section - queue-level
/// session-processor overrides for <see cref="InventoryStateChangedServiceBusHostedService"/>. Leave
/// <see cref="MaxConcurrentSessions"/>/<see cref="MaxConcurrentCallsPerSession"/> unset for the common
/// case of this queue sharing the top-level <c>ServiceBus</c> section's values via
/// <see cref="ApplyServiceBusLevelDefaults"/>; set them here only when this queue specifically needs
/// a different concurrency profile.
/// </summary>
public sealed class InventoryStateChangedServiceBusConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "ServiceBus:InventoryStateChanged";

    /// <summary>Queue level falls back to the ServiceBus-level value if unset - see <see cref="ApplyServiceBusLevelDefaults"/>.</summary>
    public int? MaxConcurrentSessions { get; set; }

    /// <summary>Queue level falls back to the ServiceBus-level value if unset - see <see cref="ApplyServiceBusLevelDefaults"/>.</summary>
    public int? MaxConcurrentCallsPerSession { get; set; }

    /// <summary>
    /// Fills <see cref="MaxConcurrentSessions"/> and <see cref="MaxConcurrentCallsPerSession"/> from
    /// <paramref name="serviceBusLevelOptions"/> wherever this (queue-level) instance left them unset -
    /// queue level wins whenever it's configured, ServiceBus level is only the fallback. Called once
    /// from an <see cref="Microsoft.Extensions.Options.IPostConfigureOptions{TOptions}"/> registration
    /// (see <see cref="MessagingServiceCollectionExtensions.RegisterInventoryEventsServiceBusConsumer"/>),
    /// after both sections have been bound and after <paramref name="serviceBusLevelOptions"/> has
    /// already had its own defaults applied if unset.
    /// </summary>
    /// <param name="serviceBusLevelOptions">The resolved top-level <c>ServiceBus</c> section options this queue's unset settings fall back to.</param>
    public void ApplyServiceBusLevelDefaults(ServiceBusConsumerOptions serviceBusLevelOptions)
    {
        MaxConcurrentSessions ??= serviceBusLevelOptions.MaxConcurrentSessions;
        MaxConcurrentCallsPerSession ??= serviceBusLevelOptions.MaxConcurrentCallsPerSession;
    }
}
