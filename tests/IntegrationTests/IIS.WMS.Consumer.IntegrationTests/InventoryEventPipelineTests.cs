using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.DependencyInjection;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.DependencyInjection;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;
using IIS.WMS.Consumer.IntegrationTests.Configuration;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles.Cosmos;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.IntegrationTests;

/// <summary>
/// End-to-end pipeline test: a Service Bus <c>InventoryStateChanged</c> envelope →
/// <see cref="InventoryStateChangedServiceBusHostedService"/> → <see cref="ItemStockInventoryRepository"/>
/// in Cosmos DB (integration-resiliency.instructions.md §9). This deliberately starts at the Service Bus
/// leg rather than a real Kafka broker: <see cref="InventoryStateChangedServiceBusHostedService"/>/
/// <c>InventoryStateChangedHandler</c> only ever handle pick/unpick transitions (real Kafka routing), so
/// there is no Create/Reserve round trip through this class to exercise - the previous version of this
/// file asserted exactly that round trip and has been rewritten. A real Avro/Schema-Registry Kafka leg
/// would need a Schema Registry Testcontainers module this repo doesn't have and isn't approved to add
/// (CLAUDE.md's no-new-NuGet-package rule) - <see cref="KafkaRelayContainerTests"/> already covers the
/// JSON Kafka leg in isolation and flags the full path as follow-up work, so bypassing it here (driving
/// the envelope directly through <see cref="ServiceBusClient"/>) is the pragmatic boundary for this suite.
/// Service Bus and Cosmos DB (and Blob Storage) each independently default to an in-process fake
/// (<see cref="VirtualServiceBusClient"/>/<see cref="InMemoryCosmosContainer"/>/<see cref="InMemoryFileStore"/>)
/// but can be pointed at a real backend instead via <see cref="IntegrationTestBackendOptions"/> - see
/// <see cref="BuildConfiguration"/> for how that's configured.
/// </summary>
/// <remarks>
/// TODO(ai): unlike the JSON Create/Reserve pipeline this replaces, <see cref="IItemStockInventoryService"/>
/// (previously <c>ItemStockInventoryService</c>, resolved indirectly through <c>InventoryStateChangedHandler</c>)
/// has no message-level idempotency check - a redelivered pick/unpick message is applied a second time,
/// not a no-op. A duplicate/redelivered-message test case (docs/ai/integration-resiliency.instructions.md §9)
/// is deliberately not included here: writing one would either lock in double-application as "correct" or
/// silently assert something the code doesn't actually guarantee. See
/// docs/InventoryStateChanged-OrderTracking-Relay.md for the follow-up note.
/// </remarks>
public sealed class InventoryEventPipelineTests : IAsyncLifetime
{
    private const string QueueName = "inventory-state-changed";
    private const string FulfilmentCode = "EDC";
    private static readonly string CosmosContainerName = CosmosContainerNames.GetItemStockInventoryContainerName(FulfilmentCode);

    private IntegrationTestBackendOptions backends = default!;
    private ServiceProvider provider = default!;
    private InventoryStateChangedServiceBusHostedService serviceBusConsumer = default!;
    private VirtualServiceBusClient? virtualServiceBusClient;
    private InMemoryCosmosContainerFactory? cosmosFactory;

    public async Task InitializeAsync()
    {
        var configuration = BuildConfiguration();
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

        // Constructed directly here (rather than resolved from `provider`) so the queue name and
        // ServiceBusHealthState instance are test-local, not the DI-registered singleton. Its
        // constructor no longer builds a session processor eagerly (see its own ExecuteAsync
        // remarks), so this is safe even against VirtualServiceBusClient.
        var dependencies = new ServiceBusConsumerDependencies(
            provider.GetRequiredService<ServiceBusClient>(),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredKeyedService<IFileStore>(BlobStorageServiceCollectionExtensions.HotTierKey),
            provider.GetRequiredKeyedService<IFileStore>(BlobStorageServiceCollectionExtensions.ColdTierKey),
            provider.GetRequiredService<IOptions<BlobStorageOptions>>(),
            new ServiceBusHealthStateRegistry());

        serviceBusConsumer = new InventoryStateChangedServiceBusHostedService(
            dependencies,
            QueueName,
            Options.Create(new InventoryStateChangedServiceBusConsumerOptions()),
            provider.GetRequiredService<ILogger<InventoryStateChangedServiceBusHostedService>>());

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
        if (backends.ServiceBus != BackendMode.Fake)
        {
            await serviceBusConsumer.StopAsync(CancellationToken.None);
        }

        await serviceBusConsumer.DisposeAsync();
        await provider.DisposeAsync();
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
    /// never commit real connection strings/keys (engineering-standards.instructions.md §6). Kafka is
    /// left disabled here - see this class's own remarks on why the Kafka leg is out of scope for this
    /// suite; <c>AddKafkaConsumers</c> still registers its three consumers as inert singletons (never
    /// resolved/started), which is harmless.
    /// </summary>
    private static IConfiguration BuildConfiguration() =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["IntegrationTestBackends:Cosmos"] = nameof(BackendMode.Fake),
                ["IntegrationTestBackends:ServiceBus"] = nameof(BackendMode.Fake),
                ["IntegrationTestBackends:BlobStorage"] = nameof(BackendMode.Fake),

                ["Application:AppName"] = "IIS.WMS.Consumer.IntegrationTests",
                ["Application:AppId"] = "iis-wms-consumer-test",

                // Never actually connected to - no Kafka leg in this suite (see class remarks) - kept
                // disabled and unresolved so AddKafkaConsumers' registration is a no-op.
                ["Kafka:Enabled"] = "false",
                ["Kafka:BootstrapServers"] = "localhost:9092",

                // The only queue this suite drives - must match the real production
                // "ServiceBus:QueueName" the Kafka relay actually publishes to (see
                // k8s/kafka-consumer/configmap.yaml and docs/InventoryStateChanged-OrderTracking-Relay.md
                // for the queue-name-mismatch bug this fixed).
                ["ServiceBus:QueueName"] = QueueName,
                ["ServiceBus:ConnectionString"] = "Endpoint=sb://localhost/;SharedAccessKeyName=x;SharedAccessKey=x",
                ["CosmosDb:AccountEndpoint"] = "https://localhost:8081",
                ["CosmosDb:DatabaseName"] = "InventoryDb",
                ["CosmosDb:EmulatorKey"] = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                ["BlobStorage:Hot:AccountUri"] = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;",
                ["BlobStorage:Cold:AccountUri"] = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;",
                ["BlobStorage:RequestAuditContainerName"] = "request-audit",
                ["BlobStorage:ConsumerDeadLetterContainerName"] = "consumer-dead-letter",

                // Never actually called on this pipeline - kept only because NexusDeduplicationService's
                // typed HttpClient factory callback needs a parseable BaseUrl to construct without
                // throwing.
                ["Nexus:Deduplication:BaseUrl"] = "http://localhost:1/",
            })
            .AddJsonFile("appsettings.IntegrationTests.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

    [Fact(DisplayName = "A pick InventoryStateChanged event flows through Service Bus and applies the B2B pick mutation to the seeded Cosmos DB record")]
    public async Task HandleMessageAsync_PickEvent_AppliesB2BPickMutation()
    {
        const string fulfilmentId = FulfilmentCode;
        const string itemCode = "SKU1";
        const string countryOfOrigin = "TH";
        const string hallmark = "925";
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);

        var repository = CreateRepository();
        await repository.CreateAsync(SeedAggregate(id, fulfilmentId, itemCode, countryOfOrigin, hallmark, b2bAllocated: 10, b2bPrepared: 0));

        var payload = BuildPickJson(fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity: 4, isB2B: true, id: "state-pick-1", referenceId: "REF-PICK-1");
        await DispatchStateChangedEventAsync(payload, correlationId: "corr-pick-1", sessionId: $"{fulfilmentId}:{itemCode}");

        var mutated = await WaitForAsync(async () =>
        {
            var current = await repository.GetAsync(id, id, CancellationToken.None);
            return current?.B2BPrepared == 4 ? current : null;
        });

        Assert.NotNull(mutated);
        Assert.Equal(6, mutated!.B2BAllocated);
        Assert.Equal(4, mutated.B2BPrepared);
    }

    [Fact(DisplayName = "An unpick (Dgp) InventoryStateChanged event flows through Service Bus and applies the unpick mutation to the seeded Cosmos DB record")]
    public async Task HandleMessageAsync_UnpickEvent_AppliesUnpickMutation()
    {
        const string fulfilmentId = FulfilmentCode;
        const string itemCode = "SKU2";
        const string countryOfOrigin = "TH";
        const string hallmark = "925";
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);

        var repository = CreateRepository();
        await repository.CreateAsync(SeedAggregate(id, fulfilmentId, itemCode, countryOfOrigin, hallmark, b2bAllocated: 6, b2bPrepared: 4));

        var payload = BuildUnpickJson(fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity: 4, id: "state-unpick-1", referenceId: "REF-UNPICK-1");
        await DispatchStateChangedEventAsync(payload, correlationId: "corr-unpick-1", sessionId: $"{fulfilmentId}:{itemCode}");

        var mutated = await WaitForAsync(async () =>
        {
            var current = await repository.GetAsync(id, id, CancellationToken.None);
            return current?.B2BPrepared == 0 ? current : null;
        });

        Assert.NotNull(mutated);
        Assert.Equal(0, mutated!.B2BPrepared);
    }

    [Fact(DisplayName = "An EDC B2B pick InventoryStateChanged event on the OTHER_STORES channel flows through Service Bus and applies the B2B pick mutation, same as any other channel")]
    public async Task HandleMessageAsync_EdcB2BPickEventOnOtherStoresChannel_AppliesB2BPickMutation()
    {
        const string fulfilmentId = FulfilmentCode;
        const string itemCode = "AT38e48db4-dabf-4d65-ab06-da1696783946";
        const string countryOfOrigin = "TH";
        const string hallmark = "NON";
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);

        var repository = CreateRepository();
        await repository.CreateAsync(SeedAggregate(id, fulfilmentId, itemCode, countryOfOrigin, hallmark, b2bAllocated: 500, b2bPrepared: 0));

        var payload = BuildOtherStoresB2BPickJson(fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity: 300, id: "f2363963-1243-4e2b-a26d-b60c23d92d9a", referenceId: "");
        await DispatchStateChangedEventAsync(payload, correlationId: "corr-pick-other-stores-1", sessionId: $"{fulfilmentId}:{itemCode}");

        var mutated = await WaitForAsync(async () =>
        {
            var current = await repository.GetAsync(id, id, CancellationToken.None);
            return current?.B2BPrepared == 300 ? current : null;
        });

        Assert.NotNull(mutated);
        Assert.Equal(200, mutated!.B2BAllocated);
        Assert.Equal(300, mutated.B2BPrepared);
    }

    [Fact(DisplayName = "A forced Cosmos 412 PreconditionFailed on the pick's write is recovered by the re-read-and-reapply retry loop")]
    public async Task HandleMessageAsync_ForcedPreconditionFailed_RetriesAndSucceeds()
    {
        if (backends.Cosmos != BackendMode.Fake)
        {
            // Forcing a real backend to return a genuine 412 deterministically needs a second,
            // actually-racing writer, which this test doesn't attempt - only possible against the fake,
            // same reasoning as ItemStockInventoryConcurrencyTests's own forced-412 test.
            return;
        }

        const string fulfilmentId = FulfilmentCode;
        const string itemCode = "SKU3";
        const string countryOfOrigin = "TH";
        const string hallmark = "925";
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);

        var repository = CreateRepository();
        await repository.CreateAsync(SeedAggregate(id, fulfilmentId, itemCode, countryOfOrigin, hallmark, b2bAllocated: 10, b2bPrepared: 0));

        var container = cosmosFactory!.GetInMemoryContainer(CosmosContainerName)
            ?? throw new InvalidOperationException($"{CosmosContainerName} container was never resolved.");
        container.ForceNextConflict(id);

        var payload = BuildPickJson(fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity: 3, isB2B: true, id: "state-412-1", referenceId: "REF-412-1");
        await DispatchStateChangedEventAsync(payload, correlationId: "corr-412-1", sessionId: $"{fulfilmentId}:{itemCode}");

        var mutated = await WaitForAsync(async () =>
        {
            var current = await repository.GetAsync(id, id, CancellationToken.None);
            return current?.B2BPrepared == 3 ? current : null;
        });

        Assert.NotNull(mutated);
        Assert.Equal(7, mutated!.B2BAllocated);
        Assert.Equal(3, mutated.B2BPrepared);
    }

    /// <summary>Sends <paramref name="payloadJson"/> through the real <see cref="ServiceBusClient"/> abstraction (virtual or real, per <see cref="backends"/>), exactly as a producer would - not by reaching into the broker directly.</summary>
    private async Task DispatchStateChangedEventAsync(string payloadJson, string correlationId, string sessionId)
    {
        using var payloadDocument = JsonDocument.Parse(payloadJson);
        var envelopeJson = JsonSerializer.Serialize(new
        {
            CorrelationId = correlationId,
            AppId = "iis-wms-consumer-test",
            Type = "InventoryStateChanged",
            ReflexSchema = payloadDocument.RootElement,
            BlobPath = "",
        });

        var message = new ServiceBusMessage(BinaryData.FromString(envelopeJson))
        {
            MessageId = Guid.NewGuid().ToString(),
            SessionId = sessionId,
        };
        message.ApplicationProperties["CorrelationId"] = correlationId;

        var sender = provider.GetRequiredService<ServiceBusClient>().CreateSender(QueueName);
        await sender.SendMessageAsync(message);
    }

    /// <summary>Constructs the concrete <see cref="ItemStockInventoryRepository"/> directly (mirrors <see cref="ItemStockInventoryConcurrencyTests"/>) so tests can seed records via <c>CreateAsync</c>, which the narrower <c>IItemStockInventoryRepository</c> interface doesn't expose.</summary>
    private ItemStockInventoryRepository CreateRepository()
    {
        var correlationContext = new CorrelationContext();
        correlationContext.Set("seed", appId: "iis-wms-consumer-test", types: ["InventoryStateChanged"]);

        return new ItemStockInventoryRepository(
            provider.GetRequiredService<ICosmosContainerFactory>(),
            NullLogger<ItemStockInventoryRepository>.Instance,
            correlationContext,
            NullAuditTrailWriter.Instance);
    }

    private static ItemStockInventory SeedAggregate(
        string id, string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark, int b2bAllocated, int b2bPrepared) =>
        ItemStockInventory.Rehydrate(
            id, fulfilmentId, itemCode, countryOfOrigin, hallmark,
            b2bAvailable: 20, b2cAvailable: 20, b2cOriginal: 20, b2cExtended: 0,
            b2cAllocated: 10, b2bAllocated: b2bAllocated, b2cPrepared: 0, b2bPrepared: b2bPrepared,
            internalHallmarkAllocated: 0, inTransit: 0, b2cThreshold: 0, isExtended: false, b2bUsedShare: 0,
            inspection: 0, psc: 0, isPosm: false, modifiedUtc: DateTime.UtcNow);

    private static string BuildPickJson(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark, int quantity, bool isB2B, string id, string? referenceId) =>
        JsonSerializer.Serialize(new
        {
            Channel = InventoryEventChannel.OwnOnline,
            Id = id,
            ChangeDate = DateTime.UtcNow,
            Location = new { Id = fulfilmentId, Type = InventoryEventLocationType.Warehouse },
            Entity = "ORG-1",
            Type = isB2B ? InventoryEventChangeType.PickedB2B : InventoryEventChangeType.PickedB2C,
            FromState = new { State = InventoryEventStockState.Available, Status = InventoryEventStockStatus.Pickable },
            ToState = new { State = InventoryEventStockState.Available, Status = InventoryEventStockStatus.Prepared },
            ItemLines = new[]
            {
                new
                {
                    LineNum = "1",
                    ProductId = itemCode,
                    ItemName = "Test Item",
                    Quantity = quantity,
                    Units = "EA",
                    CountryOfOrigin = countryOfOrigin,
                    Hallmarking = hallmark,
                },
            },
            ReferenceId = referenceId,
        });

    /// <summary>
    /// Mirrors a real-world EDC B2B pick sample on the <see cref="InventoryEventChannel.OtherStores"/>
    /// channel (wire value <c>"OTHER_STORES"</c>) - the handler dispatches the pick mutation purely on
    /// <c>message.Type</c> (see <see cref="InventoryStateChangedHandler.ApplyItemStockMutationsAsync"/>),
    /// so this channel is expected to behave identically to <see cref="BuildPickJson"/>'s
    /// <see cref="InventoryEventChannel.OwnOnline"/> case; this test proves that rather than assuming it.
    /// Several item-line fields are left <see langword="null"/>, matching the sample - none of them feed
    /// the mutation (only <c>ProductId</c>/<c>Quantity</c>/<c>CountryOfOrigin</c>/<c>Hallmarking</c> do).
    /// </summary>
    private static string BuildOtherStoresB2BPickJson(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark, int quantity, string id, string? referenceId) =>
        JsonSerializer.Serialize(new
        {
            Channel = InventoryEventChannel.OtherStores,
            Id = id,
            ChangeDate = DateTime.UtcNow,
            Location = new { Id = fulfilmentId, Type = InventoryEventLocationType.Warehouse },
            Entity = "test",
            Type = InventoryEventChangeType.PickedB2B,
            FromState = new { State = InventoryEventStockState.Available, Status = InventoryEventStockStatus.Pickable },
            ToState = new { State = InventoryEventStockState.Available, Status = InventoryEventStockStatus.Prepared },
            ItemLines = new[]
            {
                new
                {
                    LineNum = "123",
                    ProductId = itemCode,
                    ItemName = (string?)null,
                    Quantity = quantity,
                    Units = (string?)null,
                    CountryOfOrigin = countryOfOrigin,
                    Hallmarking = hallmark,
                    NetWeight = (object?)null,
                    TareWeight = (object?)null,
                    UnitPrice = (object?)null,
                    CommodityCode = (string?)null,
                    ItemCategoryLocalized = (string?)null,
                    ItemMaterialNameLocalized = (string?)null,
                    InventoryRegistrationId = (string?)null,
                    CustomsRegistrationLineNum = (string?)null,
                    IsBonded = (bool?)null,
                },
            },
            ReferenceId = referenceId,
        });

    private static string BuildUnpickJson(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark, int quantity, string id, string? referenceId) =>
        JsonSerializer.Serialize(new
        {
            Channel = InventoryEventChannel.OwnOnline,
            Id = id,
            ChangeDate = DateTime.UtcNow,
            Location = new { Id = fulfilmentId, Type = InventoryEventLocationType.Warehouse },
            Entity = "ORG-1",
            Type = InventoryEventChangeType.Dgp,
            FromState = new { State = InventoryEventStockState.Available, Status = InventoryEventStockStatus.Prepared },
            ToState = new { State = InventoryEventStockState.Available, Status = InventoryEventStockStatus.Held },
            ItemLines = new[]
            {
                new
                {
                    LineNum = "1",
                    ProductId = itemCode,
                    ItemName = "Test Item",
                    Quantity = quantity,
                    Units = "EA",
                    CountryOfOrigin = countryOfOrigin,
                    Hallmarking = hallmark,
                },
            },
            ReferenceId = referenceId,
        });

    /// <summary>Polls <paramref name="check"/> until it returns a non-null result or the timeout elapses - the Service Bus → Cosmos flow is asynchronous background work, not something a single synchronous check can observe.</summary>
    private static async Task<ItemStockInventory?> WaitForAsync(
        Func<Task<ItemStockInventory?>> check, TimeSpan? timeout = null)
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
