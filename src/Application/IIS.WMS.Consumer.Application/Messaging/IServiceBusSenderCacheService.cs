using IIS.WMS.Consumer.Application.Messaging.Dtos;

namespace IIS.WMS.Consumer.Application.Messaging;

/// <summary>
/// Lists and clears the cached <c>ServiceBusSender</c>s every Kafka relay consumer running in this
/// process holds - one sender per distinct Service Bus queue actually used
/// (integration-resiliency.instructions.md §1), reused for the lifetime of the app per Microsoft's
/// Service Bus client-lifetime guidance. Implemented in Infrastructure (<c>ConsumerHostedService</c>
/// owns the caches themselves); this interface is what lets the Api layer's admin endpoint reach them
/// without depending on Infrastructure directly, per the Api → Application → Domain dependency rule
/// (dotnet-architecture-good-practices.instructions.md). Covers only the consumers running in the
/// same process as the caller - in the current single-process skeleton that's every Kafka relay
/// consumer; once the target 3-Deployment split
/// (kubernetes-deployment-best-practices.instructions.md) separates the Kafka consumer into its own
/// Pod, this service (and the admin endpoint built on it) needs to move there too, the same way each
/// Pod's own <c>/health/ready</c> already only reports on that process's own dependencies.
/// </summary>
public interface IServiceBusSenderCacheService
{
    /// <summary>One entry per Kafka relay consumer registered in this process, listing the Service Bus queue names it currently has a cached sender for.</summary>
    IReadOnlyList<ServiceBusSenderCacheEntry> ListCachedSenders();

    /// <summary>
    /// Disposes and evicts every cached sender across every registered consumer - each one re-opens a
    /// fresh sender for its queue the next time it publishes. Safe to call while consumers are
    /// actively relaying; see <c>ConsumerHostedService.ClearServiceBusSendersAsync</c> for why an
    /// in-flight publish isn't affected.
    /// </summary>
    Task ClearCachedSendersAsync(CancellationToken cancellationToken);
}
