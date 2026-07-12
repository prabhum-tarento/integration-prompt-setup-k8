using Azure.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Infrastructure.BlobStorage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Polly.Registry;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Facade bundling the dependencies every <see cref="ConsumerHostedService"/> subclass needs and
/// which never vary from one consumer to the next - the same <see cref="ServiceBusClient"/>,
/// <see cref="ResiliencePipelineProvider{TKey}"/>, hot/cold <see cref="IFileStore"/> pair,
/// <see cref="BlobStorage.BlobStorageOptions"/>, and <see cref="IDeduplicationService"/> instance is
/// injected into every consumer today. Introduced to cut constructor over-injection (each consumer
/// was individually re-declaring and forwarding all six to its own base call) - what's deliberately
/// <b>not</b> folded in here is anything that genuinely varies per consumer: <c>ILogger&lt;T&gt;</c>
/// (needs the concrete derived type for its category name), the keyed <see cref="ConsumerHealthState"/>
/// (a distinct instance per consumer), and consumer-specific collaborators like
/// <see cref="ISpecificRecordDeserializerFactory"/> or a schema's own <c>IValidator&lt;T&gt;</c>.
/// Registered as a singleton - safe to share since every member is itself already a singleton.
/// </summary>
public sealed class ConsumerRelayInfrastructure(
    ServiceBusClient serviceBusClient,
    ResiliencePipelineProvider<string> pipelineProvider,
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.HotTierKey)] IFileStore hotFileStore,
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.ColdTierKey)] IFileStore coldFileStore,
    IOptions<BlobStorageOptions> blobStorageOptions,
    IDeduplicationService deduplicationService)
{
    /// <summary>Client used to create the sender for each consumer's relay queue.</summary>
    public ServiceBusClient ServiceBusClient { get; } = serviceBusClient;

    /// <summary>Resolves the named Polly pipeline used for the Service Bus publish step.</summary>
    public ResiliencePipelineProvider<string> PipelineProvider { get; } = pipelineProvider;

    /// <summary>Writes the hot-tier (failure/dead-letter) audit blobs - a separate Storage account from <see cref="ColdFileStore"/>.</summary>
    public IFileStore HotFileStore { get; } = hotFileStore;

    /// <summary>Writes the cold-tier (every message, success audit) blobs - a separate Storage account from <see cref="HotFileStore"/>.</summary>
    public IFileStore ColdFileStore { get; } = coldFileStore;

    /// <summary>Configured cold-tier/hot-tier audit container names.</summary>
    public IOptions<BlobStorageOptions> BlobStorageOptions { get; } = blobStorageOptions;

    /// <summary>Checks each message against the Nexus deduplication service.</summary>
    public IDeduplicationService DeduplicationService { get; } = deduplicationService;
}
