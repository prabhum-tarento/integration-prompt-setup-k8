using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Confluent.Kafka;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.DependencyInjection;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Infrastructure.DependencyInjection;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.IntegrationTests.Configuration;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles.Cosmos;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Kafka;

namespace IIS.WMS.Consumer.IntegrationTests;

/// <summary>
/// End-to-end pipeline test: Kafka (Testcontainers, always a real broker) → Service Bus → Cosmos DB
/// (integration-resiliency.instructions.md §9). Service Bus and Cosmos (and Blob Storage, incidentally
/// exercised by the Kafka relay's cold-tier audit write) each independently default to an in-process
/// fake (<see cref="VirtualServiceBusClient"/>/<see cref="InMemoryCosmosContainer"/>/
/// <see cref="InMemoryFileStore"/>) but can be pointed at a real backend instead via
/// <see cref="IntegrationTestBackendOptions"/> - see <see cref="BuildConfiguration"/> for how that's
/// configured. This is what replaces the previously-incomplete Testcontainers approach for those two
/// legs (<see cref="KafkaRelayContainerTests"/>'s own remarks note neither emulator was ever wired up).
/// </summary>
public sealed class InventoryEventPipelineTests : IAsyncLifetime
{
    private readonly KafkaContainer kafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:7.5.12").Build();
    private const string Topic = "inventory-events";
    private const string QueueName = "inventory-events";
    private const string CosmosContainerName = "InventoryEvents";

    private IntegrationTestBackendOptions backends = default!;
    private ServiceProvider provider = default!;
    private KafkaConsumerHostedService kafkaConsumer = default!;
    private ServiceBusConsumerHostedService serviceBusConsumer = default!;
    private VirtualServiceBusClient? virtualServiceBusClient;
    private InMemoryCosmosContainerFactory? cosmosFactory;

    public async Task InitializeAsync()
    {
        await kafkaContainer.StartAsync();

        var configuration = BuildConfiguration(kafkaContainer.GetBootstrapAddress());
        backends = configuration.GetSection(IntegrationTestBackendOptions.SectionName).Get<IntegrationTestBackendOptions>()
            ?? new IntegrationTestBackendOptions();

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));

        // Only needed by AddCosmosDb/AddBlobStorage's real client factories (IsDevelopment() decides
        // emulator-key vs. DefaultAzureCredential) - harmless to register even when every dependency
        // below ends up running as its in-process fake instead.
        services.AddSingleton<IHostEnvironment>(new IntegrationTestHostEnvironment());

        services.AddApplication();
        services.AddInfrastructure(configuration);

        // --- Per-dependency: swap in the in-process fake, or leave AddInfrastructure's real,
        // connection-string-driven registration in place - see BuildConfiguration's remarks. ---

        if (backends.ServiceBus == BackendMode.Fake)
        {
            virtualServiceBusClient = new VirtualServiceBusClient();
            services.RemoveAll<ServiceBusClient>();
            services.AddSingleton<ServiceBusClient>(virtualServiceBusClient);
        }

        if (backends.Cosmos == BackendMode.Fake)
        {
            cosmosFactory = new InMemoryCosmosContainerFactory();
            services.RemoveAll<ICosmosContainerFactory>();
            services.AddSingleton<ICosmosContainerFactory>(cosmosFactory);
        }

        if (backends.BlobStorage == BackendMode.Fake)
        {
            var fileStore = new InMemoryFileStore();
            services.AddKeyedSingleton<IFileStore>(BlobStorageServiceCollectionExtensions.HotTierKey, fileStore);
            services.AddKeyedSingleton<IFileStore>(BlobStorageServiceCollectionExtensions.ColdTierKey, fileStore);
        }

        provider = services.BuildServiceProvider();

        // The Kafka relay is registered (and resolvable) through the real AddMessaging/AddKafkaConsumer
        // wiring regardless of backend mode - only the dependencies it publishes/writes through vary.
        kafkaConsumer = provider.GetRequiredService<KafkaConsumerHostedService>();
        await kafkaConsumer.StartAsync(CancellationToken.None);

        // ServiceBusConsumerHostedService's own DI registration is currently gated off
        // (MessagingServiceCollectionExtensions.AddMessaging's `if (false)` block - pre-existing,
        // unrelated to this test), so it's constructed directly here instead of resolved from `provider`.
        // Its constructor no longer builds a session processor eagerly (see its own ExecuteAsync
        // remarks), so this is safe even against VirtualServiceBusClient.
        serviceBusConsumer = new ServiceBusConsumerHostedService(
            provider.GetRequiredService<ServiceBusClient>(),
            Options.Create(new ServiceBusConsumerOptions { QueueName = QueueName }),
            provider.GetRequiredService<IServiceScopeFactory>(),
            new ServiceBusHealthState(),
            provider.GetRequiredService<ILogger<ServiceBusConsumerHostedService>>());

        if (backends.ServiceBus == BackendMode.Fake)
        {
            // Message delivery goes through HandleMessageAsync directly via the broker - ExecuteAsync
            // (and the real ServiceBusSessionProcessor it would build) is never invoked in this mode.
            virtualServiceBusClient!.Broker.RegisterQueue(
                QueueName, (message, cancellationToken) => serviceBusConsumer.HandleMessageAsync(message, cancellationToken));
        }
        else
        {
            // Real queue - drive it through the actual ServiceBusSessionProcessor this starts.
            await serviceBusConsumer.StartAsync(CancellationToken.None);
        }
    }

    public async Task DisposeAsync()
    {
        await kafkaConsumer.StopAsync(CancellationToken.None);

        if (backends.ServiceBus != BackendMode.Fake)
        {
            await serviceBusConsumer.StopAsync(CancellationToken.None);
        }

        await serviceBusConsumer.DisposeAsync();
        await provider.DisposeAsync();
        await kafkaContainer.DisposeAsync();
    }

    /// <summary>
    /// Layers configuration lowest-to-highest precedence: in-process-only defaults (every
    /// <see cref="IntegrationTestBackendOptions"/> toggle left at <see cref="BackendMode.Fake"/>, plus
    /// placeholder connection details that are never actually reached in that mode) →
    /// <c>appsettings.IntegrationTests.json</c> (optional, not checked in with real secrets) →
    /// environment variables. To point a dependency at a real backend instead: set
    /// <c>IntegrationTestBackends:{Cosmos|ServiceBus|BlobStorage}=ConnectionString</c> and supply that
    /// dependency's real <c>CosmosDb</c>/<c>ServiceBus</c>/<c>BlobStorage</c> section values (e.g. via
    /// environment variables in CI, or a local, gitignored <c>appsettings.IntegrationTests.json</c>) -
    /// never commit real connection strings/keys (engineering-standards.instructions.md §6).
    /// </summary>
    private static IConfiguration BuildConfiguration(string kafkaBootstrapAddress) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["IntegrationTestBackends:Cosmos"] = nameof(BackendMode.Fake),
                ["IntegrationTestBackends:ServiceBus"] = nameof(BackendMode.Fake),
                ["IntegrationTestBackends:BlobStorage"] = nameof(BackendMode.Fake),

                ["Application:AppName"] = "IIS.WMS.Consumer.IntegrationTests",
                ["Application:AppId"] = "iis-wms-consumer-test",

                // Real (Testcontainers) Kafka broker - only the InventoryEvents JSON consumer is
                // enabled (KafkaEventFunctions allow-list) so the Avro/bulk-import consumers, which
                // need a working Schema Registry, are never registered at all.
                ["Kafka:Enabled"] = "true",
                ["Kafka:BootstrapServers"] = kafkaBootstrapAddress,
                ["Kafka:Topic"] = Topic,
                ["Kafka:ConsumerGroup"] = $"pipeline-test-{Guid.NewGuid():N}",
                ["Kafka:ServiceBusQueueName"] = QueueName,
                ["Kafka:DeduplicationCheckEnabled"] = "false",
                ["Kafka:KafkaEventFunctions:0"] = KafkaEvents.InventoryEventsConsumerKey,

                // Placeholders - only actually read when the matching IntegrationTestBackends toggle
                // above is overridden to ConnectionString; ignored while running as the in-process fake.
                ["ServiceBus:QueueName"] = QueueName,
                ["ServiceBus:ConnectionString"] = "Endpoint=sb://localhost/;SharedAccessKeyName=x;SharedAccessKey=x",
                ["CosmosDb:AccountEndpoint"] = "https://localhost:8081",
                ["CosmosDb:DatabaseName"] = "InventoryDb",
                ["CosmosDb:EmulatorKey"] = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                ["BlobStorage:Hot:AccountUri"] = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;",
                ["BlobStorage:Cold:AccountUri"] = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;",
                ["BlobStorage:RequestAuditContainerName"] = "request-audit",
                ["BlobStorage:ConsumerDeadLetterContainerName"] = "consumer-dead-letter",

                // Never actually called - Kafka:DeduplicationCheckEnabled above skips the dedup check
                // entirely, but NexusDeduplicationService's typed HttpClient factory callback still
                // needs a parseable BaseUrl to construct without throwing.
                ["Nexus:Deduplication:BaseUrl"] = "http://localhost:1/",
            })
            .AddJsonFile("appsettings.IntegrationTests.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

    [Fact(DisplayName = "A Kafka inventory event flows through Service Bus into Cosmos DB, including redelivery and a forced concurrency conflict")]
    public async Task FullPipeline_KafkaEventThroughServiceBusAndCosmos_AppliesCorrectly()
    {
        const string warehouseId = "WH1";
        const string sku = "SKU1";
        var category = $"{warehouseId}:{sku}";

        using var repositoryScope = provider.CreateScope();
        var repository = repositoryScope.ServiceProvider.GetRequiredService<IInventoryEventRepository>();

        // --- Happy path: a Create event lands in Cosmos DB ---
        await ProduceAsync(new { EventId = "evt-create-1", WarehouseId = warehouseId, Sku = sku, Quantity = 10, EventType = "Create" });

        var created = await WaitForAsync(() => repository.GetAsync(category, category));
        Assert.NotNull(created);
        Assert.Equal(10, created!.OnHandQuantity);

        // --- Duplicate/redelivered Create: same deterministic id, applied twice - a no-op the second
        // time (cosmos-db.instructions.md §5's Conflict-treated-as-already-applied path), not a failure
        // or a double-create. ---
        await ProduceAsync(new { EventId = "evt-create-1", WarehouseId = warehouseId, Sku = sku, Quantity = 10, EventType = "Create" });
        await Task.Delay(TimeSpan.FromSeconds(2)); // let the redelivered message flow through; no new item to wait on

        var afterDuplicate = await repository.GetAsync(category, category);
        Assert.Equal(10, afterDuplicate!.OnHandQuantity);

        // --- Forced Cosmos 412 PreconditionFailed on the first Reserve write - proves
        // InventoryEventService.ReserveStockAsync's re-read-and-reapply loop
        // (integration-resiliency.instructions.md §2) actually recovers, rather than failing the
        // message. Only possible in Fake mode: forcing a real backend to return a genuine 412
        // deterministically needs a second, actually-racing writer, which this test doesn't attempt -
        // the retry loop itself still has dedicated unit test coverage regardless of backend mode. ---
        if (backends.Cosmos == BackendMode.Fake)
        {
            var container = cosmosFactory!.GetInMemoryContainer(CosmosContainerName)
                ?? throw new InvalidOperationException($"{CosmosContainerName} container was never resolved.");
            container.ForceNextConflict(category);
        }

        await ProduceAsync(new { EventId = "evt-reserve-1", WarehouseId = warehouseId, Sku = sku, Quantity = 3, EventType = "Reserve" });

        var afterReserve = await WaitForAsync(async () =>
        {
            var current = await repository.GetAsync(category, category);
            return current?.OnHandQuantity == 7 ? current : null;
        });
        Assert.NotNull(afterReserve);
        Assert.Equal(7, afterReserve!.OnHandQuantity);
        Assert.Contains("evt-reserve-1", afterReserve.ActiveReservations.Keys);
    }

    private async Task ProduceAsync(object payload)
    {
        using var producer = new ProducerBuilder<string, string>(
            new ProducerConfig { BootstrapServers = kafkaContainer.GetBootstrapAddress() }).Build();

        await producer.ProduceAsync(Topic, new Message<string, string>
        {
            Value = JsonSerializer.Serialize(payload),
        });
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    /// <summary>Polls <paramref name="check"/> until it returns a non-null result or the timeout elapses - the Kafka → Service Bus → Cosmos flow is asynchronous background work, not something a single synchronous check can observe.</summary>
    private static async Task<InventoryEvent?> WaitForAsync(
        Func<Task<InventoryEvent?>> check, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));

        while (DateTime.UtcNow < deadline)
        {
            var result = await check();
            if (result is not null)
            {
                return result;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }

        return null;
    }
}
