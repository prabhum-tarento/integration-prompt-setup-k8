using Azure.Messaging.ServiceBus;
using IIS.WMS.Common.BlobStorage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Common.Messaging.ServiceBus;

/// <summary>
/// Bundles the plumbing dependencies every <see cref="ServiceBusConsumerHostedService{TMessage}"/>
/// needs, cutting constructor over-injection on both the base class and its derived classes - the
/// Service Bus mirror of Kafka's own <c>ConsumerRelayInfrastructure</c>. What's deliberately not
/// folded in here is anything that varies per queue - the queue name itself, and the resolved
/// <c>MaxConcurrentSessions</c>/<c>MaxConcurrentCallsPerSession</c> values - which stay separate
/// constructor parameters on each derived hosted service, the same way Kafka's per-event
/// <c>IOptions&lt;InventoryStateChangedConsumerOptions&gt;</c> stays outside <c>ConsumerRelayInfrastructure</c>.
/// <see cref="ServiceBusHealthStateRegistry"/> IS folded in, since - unlike the queue name - the
/// registry itself is identical for every queue; only the lookup key differs per consumer.
/// </summary>
public sealed class ServiceBusConsumerDependencies(
    ServiceBusClient client,
    IServiceScopeFactory scopeFactory,
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.HotTierKey)] IFileStore hotFileStore,
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.ColdTierKey)] IFileStore coldFileStore,
    IOptions<BlobStorageOptions> blobStorageOptions,
    ServiceBusHealthStateRegistry healthStateRegistry)
{
    /// <summary>Service Bus client the session processor is built from, on first start.</summary>
    public ServiceBusClient Client { get; } = client;

    /// <summary>Creates a DI scope per message, since the message handler is scoped.</summary>
    public IServiceScopeFactory ScopeFactory { get; } = scopeFactory;

    /// <summary>Hot-tier file store for dead-letter blob writes.</summary>
    public IFileStore HotFileStore { get; } = hotFileStore;

    /// <summary>Cold-tier file store for the request-audit blob write.</summary>
    public IFileStore ColdFileStore { get; } = coldFileStore;

    /// <summary>Container names for the dead-letter and request-audit blob writes.</summary>
    public IOptions<BlobStorageOptions> BlobStorageOptions { get; } = blobStorageOptions;

    /// <summary>Process-wide registry this consumer resolves its own <see cref="ServiceBusHealthState"/> from, keyed by queue name.</summary>
    public ServiceBusHealthStateRegistry HealthStateRegistry { get; } = healthStateRegistry;
}
