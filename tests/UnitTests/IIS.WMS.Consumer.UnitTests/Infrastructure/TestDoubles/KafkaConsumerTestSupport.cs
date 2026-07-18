using System.Reflection;
using System.Text;
using Confluent.Kafka;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using IIS.WMS.Consumer.Infrastructure.Messaging.OrderArchiving;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure.TestDoubles;

/// <summary>
/// Shared scaffolding for testing <see cref="ConsumerHostedService"/> subclasses without a live Kafka
/// broker or Service Bus. <see cref="ConsumerHostedService"/> builds its own real (but never actually
/// connected) Confluent.Kafka <c>IConsumer</c> in its constructor and has no constructor seam to
/// substitute it - so these tests invoke its private per-message flow (<c>ProcessMessageAsync</c>)
/// directly via reflection instead of driving it through <c>ExecuteAsync</c>'s poll loop. This is safe
/// because <c>ProcessMessageAsync</c> never calls <c>IConsumer.Consume</c>/<c>Subscribe</c> itself, and
/// because <c>PartitionOffsetCommitTracker.Complete</c> is a documented no-op (see
/// <c>PartitionOffsetCommitTrackerTests</c>) when no baseline was ever established for that partition -
/// which only <c>ExecuteAsync</c>'s poll loop (via <c>EstablishBaseline</c>) ever does - so the real
/// <c>consumer.Commit(...)</c> call the tracker would otherwise issue is never actually reached by
/// calling <c>ProcessMessageAsync</c> directly.
/// </summary>
internal static class KafkaConsumerTestSupport
{
    /// <summary>Every mock/fake <see cref="ConsumerRelayInfrastructure"/> wraps, so a test can configure/assert on each independently.</summary>
    public sealed record RelayInfrastructureFixture(
        ConsumerRelayInfrastructure Infrastructure,
        IServiceBusRelayPublisher RelayPublisher,
        IFileStore HotFileStore,
        IFileStore ColdFileStore,
        BlobStorageOptions BlobStorageOptions,
        IDeduplicationService DeduplicationService,
        IDynamicEventValidator DynamicEventValidator,
        IOrderArchiveRepository OrderArchiveRepository,
        IOrderArchiveWriter OrderArchiveWriter);

    /// <summary>
    /// Builds a <see cref="ConsumerRelayInfrastructure"/> backed entirely by mocks/fakes, each
    /// defaulted to the "happy path" (dedup says not-a-duplicate, dynamic validation passes, Service
    /// Bus publish succeeds) - override individual members via the returned fixture's properties for a
    /// specific test.
    /// </summary>
    public static RelayInfrastructureFixture CreateInfrastructure(BlobStorageOptions? blobStorageOptions = null)
    {
        var relayPublisher = Substitute.For<IServiceBusRelayPublisher>();
        relayPublisher.PublishAsync(Arg.Any<ServiceBusRelayMessage>(), Arg.Any<CancellationToken>())
            .Returns(new ServiceBusRelayPublishResult(WasOffloaded: false, BlobPath: string.Empty, TimeSpan.Zero, TimeSpan.FromMilliseconds(1)));

        var hotFileStore = Substitute.For<IFileStore>();
        var coldFileStore = Substitute.For<IFileStore>();

        var deduplicationService = Substitute.For<IDeduplicationService>();
        deduplicationService
            .IsDuplicateAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(false);

        var dynamicEventValidator = Substitute.For<IDynamicEventValidator>();
        dynamicEventValidator
            .ValidateAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<object>(), Arg.Any<HeaderLookup?>(), Arg.Any<ILogger>(), Arg.Any<IServiceProvider>(), Arg.Any<CancellationToken>())
            .Returns(true);

        var orderArchiveRepository = Substitute.For<IOrderArchiveRepository>();
        var orderArchiveWriter = Substitute.For<IOrderArchiveWriter>();

        var services = new ServiceCollection();
        services.AddScoped<ICorrelationContext, CorrelationContext>();
        services.AddSingleton(orderArchiveRepository);
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

        var applicationOptions = Options.Create(new ApplicationOptions { AppName = "IIS.WMS.Consumer", AppId = "iis-wms-consumer" });
        var resolvedBlobStorageOptions = blobStorageOptions ?? new BlobStorageOptions();

        var infrastructure = new ConsumerRelayInfrastructure(
            relayPublisher,
            hotFileStore,
            coldFileStore,
            Options.Create(resolvedBlobStorageOptions),
            deduplicationService,
            dynamicEventValidator,
            orderArchiveWriter,
            scopeFactory,
            applicationOptions);

        return new RelayInfrastructureFixture(
            infrastructure, relayPublisher, hotFileStore, coldFileStore, resolvedBlobStorageOptions,
            deduplicationService, dynamicEventValidator, orderArchiveRepository, orderArchiveWriter);
    }

    /// <summary>Builds a raw Kafka <see cref="ConsumeResult{TKey,TValue}"/> with the header set <see cref="ConsumerHostedService"/> reads - pass <see langword="null"/> for a header to omit it entirely (e.g. to test the "no Correlation-Id header" fallback).</summary>
    public static ConsumeResult<string, byte[]> CreateConsumeResult(
        byte[] value,
        string? correlationId = "corr-1",
        string? deduplicationId = "dedup-1",
        string? eventType = "",
        string? appId = "producer-app",
        string? key = "WH1:SKU1",
        string topic = "inventory-events",
        int partition = 0,
        long offset = 1)
    {
        var headers = new Headers();

        if (correlationId is not null)
        {
            headers.Add(WellKnownHeaderNames.CorrelationId, Encoding.UTF8.GetBytes(correlationId));
        }

        if (deduplicationId is not null)
        {
            headers.Add(WellKnownHeaderNames.DeduplicationId, Encoding.UTF8.GetBytes(deduplicationId));
        }

        if (eventType is not null)
        {
            headers.Add(WellKnownHeaderNames.Type, Encoding.UTF8.GetBytes(eventType));
        }

        if (appId is not null)
        {
            headers.Add(WellKnownHeaderNames.AppId, Encoding.UTF8.GetBytes(appId));
        }

        return new ConsumeResult<string, byte[]>
        {
            Topic = topic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<string, byte[]>
            {
                Key = key!,
                Value = value,
                Headers = headers,
                Timestamp = new Timestamp(DateTime.UtcNow),
            },
        };
    }

    private static readonly MethodInfo ProcessMessageAsyncMethod = typeof(ConsumerHostedService)
        .GetMethod("ProcessMessageAsync", BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new InvalidOperationException("ConsumerHostedService.ProcessMessageAsync not found by reflection - has its signature changed?");

    /// <summary>Invokes the private per-message flow directly - see this class's own remarks for why that's safe without a live broker.</summary>
    public static async Task ProcessMessageAsync(this ConsumerHostedService service, ConsumeResult<string, byte[]> result, CancellationToken cancellationToken = default)
    {
        var task = (Task)ProcessMessageAsyncMethod.Invoke(service, [result, cancellationToken])!;
        await task;
    }

    private static readonly FieldInfo SchemaHandlersField = typeof(ConsumerHostedService)
        .GetField("schemaHandlers", BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new InvalidOperationException("ConsumerHostedService.schemaHandlers not found by reflection - has its signature changed?");

    /// <summary>
    /// Fetches one registered schema handler by its <see cref="WellKnownHeaderNames.Type"/> key, as a
    /// loosely-typed <see cref="object"/> since the handler's own runtime type
    /// (<c>ConsumerHostedService.SchemaHandler{T}</c>/<c>MappedSchemaHandler{TAvro,TValue}</c>) is a
    /// private nested class - callers invoke its members via the reflection helpers below instead of a
    /// compile-time interface reference.
    /// </summary>
    public static object GetSchemaHandler(this ConsumerHostedService service, string eventType)
    {
        var dictionary = SchemaHandlersField.GetValue(service)!;
        var tryGetValue = dictionary.GetType().GetMethod("TryGetValue")!;
        var args = new object?[] { eventType, null };
        var found = (bool)tryGetValue.Invoke(dictionary, args)!;

        return found ? args[1]! : throw new KeyNotFoundException($"No schema handler registered under Type key '{eventType}'.");
    }

    /// <summary>Invokes a schema handler's <c>GetOrderArchiveKey(object)</c> - see <see cref="GetSchemaHandler"/>.</summary>
    public static string? InvokeGetOrderArchiveKey(this object schemaHandler, object value) =>
        (string?)schemaHandler.GetType().GetMethod("GetOrderArchiveKey")!.Invoke(schemaHandler, [value]);

    /// <summary>Reads a schema handler's <c>ServiceBusQueueName</c> - see <see cref="GetSchemaHandler"/>.</summary>
    public static string? GetServiceBusQueueNameProperty(this object schemaHandler) =>
        (string?)schemaHandler.GetType().GetProperty("ServiceBusQueueName")!.GetValue(schemaHandler);

    /// <summary>Reads a schema handler's <c>SchemaName</c> - see <see cref="GetSchemaHandler"/>.</summary>
    public static string GetSchemaNameProperty(this object schemaHandler) =>
        (string)schemaHandler.GetType().GetProperty("SchemaName")!.GetValue(schemaHandler)!;
}
