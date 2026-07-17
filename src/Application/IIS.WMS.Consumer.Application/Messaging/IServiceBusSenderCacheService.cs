using IIS.WMS.Consumer.Application.Messaging.Dtos;

namespace IIS.WMS.Consumer.Application.Messaging;

/// <summary>
/// Lists and clears the cached <c>ServiceBusSender</c>s held in this process - one sender per distinct
/// Service Bus queue actually used, across every caller (integration-resiliency.instructions.md §1),
/// reused for the lifetime of the app per Microsoft's Service Bus client-lifetime guidance. Implemented
/// in Infrastructure (the shared <c>ServiceBusRelayPublisher</c> singleton owns the cache itself, via
/// <c>IServiceBusSenderCacheSource</c>); this interface is what lets the Api layer's admin endpoint
/// reach it without depending on Infrastructure directly, per the Api → Application → Domain dependency
/// rule (dotnet-architecture-good-practices.instructions.md). Covers only this process - once the
/// target 3-Deployment split (kubernetes-deployment-best-practices.instructions.md) separates the
/// Kafka consumer into its own Pod, this service (and the admin endpoint built on it) needs to move
/// there too, the same way each Pod's own <c>/health/ready</c> already only reports on that process's
/// own dependencies.
/// </summary>
public interface IServiceBusSenderCacheService
{
    /// <summary>One entry per <c>IServiceBusSenderCacheSource</c> registered in this process - today, the one shared <c>ServiceBusRelayPublisher</c> - listing the Service Bus queue names it currently has a cached sender for.</summary>
    IReadOnlyList<ServiceBusSenderCacheEntry> ListCachedSenders();

    /// <summary>
    /// Disposes and evicts every cached sender across every registered source - each one re-opens a
    /// fresh sender for its queue the next time it publishes. Safe to call while consumers are
    /// actively relaying; see <c>ServiceBusRelayPublisher.ClearServiceBusSendersAsync</c> for why an
    /// in-flight publish isn't affected.
    /// </summary>
    Task ClearCachedSendersAsync(CancellationToken cancellationToken);
}
