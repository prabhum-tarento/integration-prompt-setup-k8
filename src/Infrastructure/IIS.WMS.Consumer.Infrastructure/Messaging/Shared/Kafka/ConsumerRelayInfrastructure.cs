using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Infrastructure.Messaging.OrderArchiving;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;

/// <summary>
/// Facade bundling the dependencies every <see cref="KafkaConsumerHostedServiceBase"/> subclass needs and
/// which never vary from one consumer to the next - the shared <see cref="IServiceBusRelayPublisher"/>,
/// hot/cold <see cref="IFileStore"/> pair, <see cref="BlobStorageOptions"/>,
/// <see cref="IDeduplicationService"/>, <see cref="IOrderArchiveWriter"/>, <see cref="IServiceScopeFactory"/>,
/// and <see cref="ApplicationOptions"/> instance is injected into every consumer today. Introduced to cut
/// constructor over-injection (each consumer was individually re-declaring and forwarding every one of
/// these to its own base call) - what's deliberately <b>not</b> folded in here is anything that
/// genuinely varies per consumer: <c>ILogger&lt;T&gt;</c> (needs the concrete derived type for its
/// category name), and consumer-specific collaborators like <see cref="ISpecificRecordDeserializerFactory"/>
/// or a schema's own <c>IValidator&lt;T&gt;</c>. <see cref="ConsumerHealthState"/> isn't here either,
/// but for a different reason - it's no longer a constructor dependency at all, see
/// <see cref="KafkaConsumerHostedServiceBase.RegisterSchemaHandlers"/>.
/// Registered as a singleton - safe to share since every member is itself already a singleton.
/// </summary>
public sealed class ConsumerRelayInfrastructure(
    IServiceBusRelayPublisher relayPublisher,
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.HotTierKey)] IFileStore hotFileStore,
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.ColdTierKey)] IFileStore coldFileStore,
    IOptions<BlobStorageOptions> blobStorageOptions,
    IDeduplicationService deduplicationService,
    IDynamicEventValidator dynamicEventValidator,
    IOrderArchiveWriter orderArchiveWriter,
    IServiceScopeFactory scopeFactory,
    IOptions<ApplicationOptions> applicationOptions)
{
    /// <summary>Shared publisher used to relay a message onto Service Bus - see its own remarks for why this is one singleton shared across every Kafka consumer rather than a per-consumer <c>ServiceBusClient</c>/sender cache.</summary>
    public IServiceBusRelayPublisher RelayPublisher { get; } = relayPublisher;

    /// <summary>Writes the hot-tier (failure/dead-letter) audit blobs - a separate Storage account from <see cref="ColdFileStore"/>.</summary>
    public IFileStore HotFileStore { get; } = hotFileStore;

    /// <summary>Writes the cold-tier (every message, success audit) blobs - a separate Storage account from <see cref="HotFileStore"/>.</summary>
    public IFileStore ColdFileStore { get; } = coldFileStore;

    /// <summary>Configured cold-tier/hot-tier audit container names.</summary>
    public IOptions<BlobStorageOptions> BlobStorageOptions { get; } = blobStorageOptions;

    /// <summary>Checks each message against the Nexus deduplication service.</summary>
    public IDeduplicationService DeduplicationService { get; } = deduplicationService;

    /// <summary>Runs the schema/event type's blob-stored validation template (if one exists) against each message, right after the schema handler's own <c>ValidateAsync</c>.</summary>
    public IDynamicEventValidator DynamicEventValidator { get; } = dynamicEventValidator;

    /// <summary>Non-blocking hand-off to the background <c>OrderArchive</c> Cosmos-write pipeline - see <see cref="IOrderArchiveWriter"/>.</summary>
    public IOrderArchiveWriter OrderArchiveWriter { get; } = orderArchiveWriter;

    /// <summary>Creates the per-message DI scope used to resolve <see cref="IIS.WMS.Common.Correlation.ICorrelationContext"/>, since the hosted service itself is a singleton but that service is scoped.</summary>
    public IServiceScopeFactory ScopeFactory { get; } = scopeFactory;

    /// <summary>This service's own identity - <see cref="ApplicationOptions.AppId"/> is the fallback <see cref="KafkaConsumerHostedServiceBase"/> uses when a relayed message's Kafka <c>App-Id</c> header is missing or empty.</summary>
    public ApplicationOptions ApplicationOptions { get; } = applicationOptions.Value;
}
