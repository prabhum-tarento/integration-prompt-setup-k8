using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.DependencyInjection;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.DependencyInjection;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryAdjusted;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryAdjusted.Handlers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Shared;
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
/// End-to-end pipeline test: a Service Bus <c>InventoryAdjusted</c> envelope →
/// <see cref="InventoryAdjustedServiceBusHostedService"/> → <see cref="InventoryAdjustedHandler"/> →
/// <see cref="ItemStockInventoryRepository"/>/<see cref="ItemStockInventoryExtendedRepository"/> in
/// Cosmos DB, and (when the §3.1 B2B gate is enabled) the SAP adjusted/moved Service Bus publish -
/// mirrors <see cref="InventoryEventPipelineTests"/>'s structure and fake/real backend-swap pattern.
/// Unlike that sibling suite, feature flags must vary per test (the §3.1 gate, isolation from
/// downstream publishers), so each test builds its own <see cref="ServiceProvider"/> via
/// <see cref="InitializeServices"/> with a per-test configuration overlay, rather than sharing one
/// provider across the whole class - <see cref="IAsyncLifetime"/>'s per-test xUnit class instantiation
/// makes this safe (no cross-test flag bleed).
/// </summary>
/// <remarks>
/// Per an explicit user directive, this suite (together with the pre-existing unit test suite -
/// already at 100% line/branch for every InventoryAdjusted-specific class) targets 100% line / 98%
/// branch coverage for the InventoryAdjusted code path specifically - a deliberate, user-authorized
/// override of engineering-standards.instructions.md §7's 98%/95% combined unit+integration target for
/// this task only (CLAUDE.md's precedence rule).
/// </remarks>
public sealed class InventoryAdjustedEventPipelineTests : IAsyncLifetime
{
    private const string QueueName = "inventory-adjusted";
    private const string FulfilmentCode = "EDC";
    private static readonly string CosmosContainerName = CosmosContainerNames.GetItemStockInventoryContainerName(FulfilmentCode);
    private static readonly string ExtendedCosmosContainerName = CosmosContainerNames.GetItemStockInventoryExtendedContainerName(FulfilmentCode);

    private IntegrationTestBackendOptions backends = default!;
    private ServiceProvider provider = default!;
    private InventoryAdjustedServiceBusHostedService serviceBusConsumer = default!;
    private VirtualServiceBusClient? virtualServiceBusClient;
    private InMemoryCosmosContainerFactory? cosmosFactory;

    public Task InitializeAsync() => InitializeServices(extraConfig: null);

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
    /// Builds this test's <see cref="ServiceProvider"/>/<see cref="serviceBusConsumer"/>, layering
    /// <paramref name="extraConfig"/> (feature-flag overrides) above <see cref="BuildConfiguration"/>'s
    /// defaults - extracted from <see cref="InitializeAsync"/> so a test needing non-default flags can
    /// rebuild its own isolated provider rather than sharing the class-wide one, since
    /// <see cref="FeatureFlagsOptions"/> is bound once as a DI singleton snapshot.
    /// </summary>
    private async Task InitializeServices(Dictionary<string, string?>? extraConfig)
    {
        var configuration = BuildConfiguration(extraConfig);
        backends = configuration.GetSection(IntegrationTestBackendOptions.SectionName).Get<IntegrationTestBackendOptions>()
            ?? new IntegrationTestBackendOptions();

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));

        services.AddSingleton<IHostEnvironment>(new IntegrationTestHostEnvironment());

        services.AddApplication();
        services.AddInfrastructure(configuration);

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

        var dependencies = new ServiceBusConsumerDependencies(
            provider.GetRequiredService<ServiceBusClient>(),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredKeyedService<IFileStore>(BlobStorageServiceCollectionExtensions.HotTierKey),
            provider.GetRequiredKeyedService<IFileStore>(BlobStorageServiceCollectionExtensions.ColdTierKey),
            provider.GetRequiredService<IOptions<BlobStorageOptions>>(),
            new ServiceBusHealthStateRegistry());

        serviceBusConsumer = new InventoryAdjustedServiceBusHostedService(
            dependencies,
            QueueName,
            Options.Create(new InventoryAdjustedServiceBusConsumerOptions()),
            provider.GetRequiredService<ILogger<InventoryAdjustedServiceBusHostedService>>());

        if (backends.ServiceBus == BackendMode.Fake)
        {
            virtualServiceBusClient!.Broker.RegisterQueue(
                QueueName, (message, cancellationToken) => serviceBusConsumer.HandleMessageAsync(message, cancellationToken));
        }
        else
        {
            await serviceBusConsumer.StartAsync(CancellationToken.None);
        }
    }

    /// <summary>Rebuilds this test's provider/consumer with <paramref name="extraConfig"/> layered above the defaults, disposing whatever <see cref="InitializeAsync"/> already built - used by tests that need non-default feature flags.</summary>
    private async Task ReinitializeWithAsync(Dictionary<string, string?> extraConfig)
    {
        await DisposeAsync();
        await InitializeServices(extraConfig);
    }

    /// <summary>
    /// Layers configuration lowest-to-highest precedence: in-process-only defaults →
    /// <paramref name="extraConfig"/> (per-test feature-flag overrides) → <c>appsettings.IntegrationTests.json</c>
    /// (optional, not checked in with real secrets) → environment variables. See
    /// <see cref="InventoryEventPipelineTests.BuildConfiguration"/> for the shared-defaults rationale;
    /// this suite additionally requires <c>InventoryPublish:*</c> queue names (all four are
    /// <c>required</c> on <see cref="InventoryPublishOptions"/> with no default - <c>InventoryAdjustedHandler</c>
    /// can reach the B2B/OMS/ICR publishers directly, unlike the sibling suite's pick/unpick-only path)
    /// and <c>FeatureFlags:*</c> (all default <see langword="false"/>, matching <see cref="FeatureFlagsOptions"/>'
    /// own default, overridden per-test via <paramref name="extraConfig"/>).
    /// </summary>
    private static IConfiguration BuildConfiguration(Dictionary<string, string?>? extraConfig)
    {
        var builder = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["IntegrationTestBackends:Cosmos"] = nameof(BackendMode.Fake),
                ["IntegrationTestBackends:ServiceBus"] = nameof(BackendMode.Fake),
                ["IntegrationTestBackends:BlobStorage"] = nameof(BackendMode.Fake),

                ["Application:AppName"] = "IIS.WMS.Consumer.IntegrationTests",
                ["Application:AppId"] = "iis-wms-consumer-test",

                ["Kafka:Enabled"] = "false",
                ["Kafka:BootstrapServers"] = "localhost:9092",

                ["ServiceBus:QueueName"] = QueueName,
                ["ServiceBus:ConnectionString"] = "Endpoint=sb://localhost/;SharedAccessKeyName=x;SharedAccessKey=x",
                ["CosmosDb:AccountEndpoint"] = "https://localhost:8081",
                ["CosmosDb:DatabaseName"] = "InventoryDb",
                ["CosmosDb:EmulatorKey"] = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                ["BlobStorage:Hot:AccountUri"] = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;",
                ["BlobStorage:Cold:AccountUri"] = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;",
                ["BlobStorage:RequestAuditContainerName"] = "request-audit",
                ["BlobStorage:ConsumerDeadLetterContainerName"] = "consumer-dead-letter",

                ["Nexus:Deduplication:BaseUrl"] = "http://localhost:1/",

                ["InventoryPublish:SapAdjustedOrMovedQueueName"] = "test-sap-adjusted-or-moved",
                ["InventoryPublish:OmsDeltaQueueName"] = "test-oms-delta",
                ["InventoryPublish:IcrSnapshotQueueName"] = "test-icr-snapshot",
                ["InventoryPublish:OrderTrackingQueueName"] = "test-order-tracking",

                ["FeatureFlags:EnableDeltaTowardsSap"] = "false",
                ["FeatureFlags:EnableDeltaTowardsAx123Pl"] = "false",
                ["FeatureFlags:EnableAdcDeltaTowardsAx12"] = "false",
                ["FeatureFlags:EnableDeltaTowardsOms"] = "false",
                ["FeatureFlags:EnableDeltaTowardsOms3Pl"] = "false",
                ["FeatureFlags:EnableSnapshotForIcr"] = "false",
            });

        if (extraConfig is not null)
        {
            builder = builder.AddInMemoryCollection(extraConfig);
        }

        return builder
            .AddJsonFile("appsettings.IntegrationTests.json", optional: true)
            .AddEnvironmentVariables()
            .Build();
    }

    [Fact(DisplayName = "An InventoryAdjusted event with an Available/Pickable state triggers §3.3 segmentation, mutating the seeded ItemStockInventory record")]
    public async Task HandleMessageAsync_AvailablePickableState_TriggersSegmentation()
    {
        const string fulfilmentId = FulfilmentCode;
        const string itemCode = "ADJ-SKU1";
        const string countryOfOrigin = "TH";
        const string hallmark = "925";
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);

        var repository = CreateItemStockInventoryRepository();
        await repository.CreateAsync(SeedAggregate(id, fulfilmentId, itemCode, countryOfOrigin, hallmark, b2bAvailable: 10));

        var payload = BuildAdjustedJson(
            fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity: 5,
            state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Pickable, referenceId: "REF-ADJ-1");
        await DispatchAdjustedEventAsync(payload, correlationId: "corr-adj-1", sessionId: $"{fulfilmentId}:{itemCode}");

        var mutated = await WaitForAsync(async () =>
        {
            var current = await repository.GetAsync(id, id, CancellationToken.None);
            return current?.B2BAvailable == 15 ? current : null;
        });

        Assert.NotNull(mutated);
        Assert.Equal(15, mutated!.B2BAvailable);
    }

    [Fact(DisplayName = "An InventoryAdjusted event with a non-Available/Pickable state skips §3.3 segmentation but still mutates the §3.5 extended-state record for its to-state")]
    public async Task HandleMessageAsync_NonAvailablePickableState_SkipsSegmentationButMutatesExtended()
    {
        const string fulfilmentId = FulfilmentCode;
        const string itemCode = "ADJ-SKU2";
        const string countryOfOrigin = "TH";
        const string hallmark = "NON";
        var baselineId = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);

        var repository = CreateItemStockInventoryRepository();
        var extendedRepository = CreateItemStockInventoryExtendedRepository();

        var payload = BuildAdjustedJson(
            fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity: 7,
            state: InventoryEventStockState.Blocked, status: InventoryEventStockStatus.Held, referenceId: "REF-ADJ-2");
        await DispatchAdjustedEventAsync(payload, correlationId: "corr-adj-2", sessionId: $"{fulfilmentId}:{itemCode}");

        var extended = await WaitForAsync(async () =>
        {
            var current = await extendedRepository.GetAsync(fulfilmentId, itemCode, hallmark, countryOfOrigin, State.BLOCKED, Status.HELD, CancellationToken.None);
            return current?.Qty == 7 ? current : null;
        });

        Assert.NotNull(extended);
        Assert.Equal(7, extended!.Qty);

        // The baseline (non-extended) ItemStockInventory record was never created - segmentation
        // never ran because BLOCKED/HELD isn't the Available/Pickable trigger.
        var baseline = await repository.GetAsync(baselineId, baselineId, CancellationToken.None);
        Assert.Null(baseline);
    }

    [Fact(DisplayName = "With the SAP gate enabled and a non-ADC location, a positive-quantity InventoryAdjusted event publishes on the configured SAP adjusted/moved queue with the adjustment's own Reason and state")]
    public async Task HandleMessageAsync_SapGateEnabledNonAdcLocation_PublishesOnSapQueue()
    {
        await ReinitializeWithAsync(new Dictionary<string, string?>
        {
            ["FeatureFlags:EnableDeltaTowardsSap"] = "true",
            ["FeatureFlags:EnableAdcDeltaTowardsAx12"] = "false",
        });

        const string fulfilmentId = "CAECOM";
        const string itemCode = "ADJ-SKU3";
        const string countryOfOrigin = "TH";
        const string hallmark = "NON";

        var payload = BuildAdjustedJson(
            fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity: 3,
            state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Pickable, referenceId: "REF-ADJ-3",
            reason: InventoryEventReasonCode.Adjustment, locationType: InventoryEventLocationType.ThirdPartyLogistics);
        await DispatchAdjustedEventAsync(payload, correlationId: "corr-adj-3", sessionId: $"{fulfilmentId}:{itemCode}");

        var dispatched = await WaitForDispatchAsync("test-sap-adjusted-or-moved");

        Assert.NotNull(dispatched);
        var body = DeserializeRelayedBody<InventoryAdjustedOrMovedPublishRequest>(dispatched!.Value.Message);
        Assert.NotNull(body);
        Assert.Equal(nameof(InventoryEventReasonCode.Adjustment), body!.Lines.Single().Reason);
        Assert.Equal("AVAILABLE", body.ToState.State);
        Assert.Equal("PICKABLE", body.ToState.Status);
        Assert.Equal("AVAILABLE", body.FromState.State);
        Assert.Equal("PICKABLE", body.FromState.Status);
    }

    [Fact(DisplayName = "With the SAP gate's SAP flag enabled but the ADC-override flag off, an InventoryAdjusted event at the ADC location does not publish on the SAP queue - proves the doc-literal two-flag gate, not InventoryStateChangedHandler's three-way OR gate")]
    public async Task HandleMessageAsync_SapGateAdcLocationWithoutOverride_DoesNotPublish()
    {
        await ReinitializeWithAsync(new Dictionary<string, string?>
        {
            ["FeatureFlags:EnableDeltaTowardsSap"] = "true",
            ["FeatureFlags:EnableAdcDeltaTowardsAx12"] = "false",
        });

        const string fulfilmentId = "ADC";
        const string itemCode = "ADJ-SKU4";
        const string countryOfOrigin = "TH";
        const string hallmark = "NON";
        var id = ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin);

        var repository = CreateItemStockInventoryRepository();
        await repository.CreateAsync(SeedAggregate(id, fulfilmentId, itemCode, countryOfOrigin, hallmark, b2bAvailable: 10));

        var payload = BuildAdjustedJson(
            fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity: 2,
            state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Pickable, referenceId: "REF-ADJ-4");
        await DispatchAdjustedEventAsync(payload, correlationId: "corr-adj-4", sessionId: $"{fulfilmentId}:{itemCode}");

        // No queue to poll for a positive signal here (the gate stays closed) - instead, wait for the
        // §3.2 segmentation side effect that always runs for an Available/Pickable adjustment (the
        // extended-record side effect the sibling scenarios use isn't available here: for this same
        // Available/Pickable baseline pair, both isValidToState and isValidFromState in
        // ItemStockInventoryExtendedSegmentationService.ApplyAsync are false, so no extended record is
        // ever written), then assert the SAP queue still received nothing. The baseline mutation landing
        // proves the message was actually processed (not just still in flight), making the following
        // "nothing published" assertion meaningful.
        var mutated = await WaitForAsync(async () =>
        {
            var current = await repository.GetAsync(id, id, CancellationToken.None);
            return current?.B2BAvailable == 12 ? current : null;
        });

        Assert.NotNull(mutated);

        var dispatched = virtualServiceBusClient!.Broker.Dispatched
            .Where(entry => entry.QueueName == "test-sap-adjusted-or-moved")
            .ToList();

        Assert.Empty(dispatched);
    }

    [Fact(DisplayName = "With the SAP gate enabled and a negative summed quantity, the published ToState/ToStatus are forced to UNKNOWN while FromState/FromStatus still reflect the adjustment's real state")]
    public async Task HandleMessageAsync_SapGateEnabledNegativeQuantity_ForcesToStateUnknown()
    {
        await ReinitializeWithAsync(new Dictionary<string, string?>
        {
            ["FeatureFlags:EnableDeltaTowardsSap"] = "true",
            ["FeatureFlags:EnableAdcDeltaTowardsAx12"] = "false",
        });

        const string fulfilmentId = "CAECOM";
        const string itemCode = "ADJ-SKU5";
        const string countryOfOrigin = "TH";
        const string hallmark = "NON";

        var payload = BuildAdjustedJson(
            fulfilmentId, itemCode, countryOfOrigin, hallmark, quantity: -4,
            state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Pickable, referenceId: "REF-ADJ-5",
            reason: InventoryEventReasonCode.Adjustment, locationType: InventoryEventLocationType.ThirdPartyLogistics);
        await DispatchAdjustedEventAsync(payload, correlationId: "corr-adj-5", sessionId: $"{fulfilmentId}:{itemCode}");

        var dispatched = await WaitForDispatchAsync("test-sap-adjusted-or-moved");

        Assert.NotNull(dispatched);
        var body = DeserializeRelayedBody<InventoryAdjustedOrMovedPublishRequest>(dispatched!.Value.Message);
        Assert.NotNull(body);
        Assert.Equal("UNKNOWN", body!.ToState.State);
        Assert.Equal("UNKNOWN", body.ToState.Status);
        Assert.Equal("AVAILABLE", body.FromState.State);
        Assert.Equal("PICKABLE", body.FromState.Status);
    }

    /// <summary>Sends <paramref name="payloadJson"/> through the real <see cref="ServiceBusClient"/> abstraction, exactly as a producer would. The envelope's <c>Type</c> is set to the exact literal <see cref="KafkaEvents.InventoryAdjustedEventType"/> string - the extended-segmentation service's SAE-3032 from-state suppression and the SAP publisher's SAE-2798 redelivery check both compare against this exact value, and the real production Kafka relay forwards this same constant as the Type header.</summary>
    private async Task DispatchAdjustedEventAsync(string payloadJson, string correlationId, string sessionId)
    {
        using var payloadDocument = JsonDocument.Parse(payloadJson);
        var envelopeJson = JsonSerializer.Serialize(new
        {
            CorrelationId = correlationId,
            AppId = "iis-wms-consumer-test",
            Type = KafkaEvents.InventoryAdjustedEventType,
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

    private ItemStockInventoryRepository CreateItemStockInventoryRepository()
    {
        var correlationContext = new CorrelationContext();
        correlationContext.Set("seed", appId: "iis-wms-consumer-test", types: [KafkaEvents.InventoryAdjustedEventType]);

        return new ItemStockInventoryRepository(
            provider.GetRequiredService<ICosmosContainerFactory>(),
            NullLogger<ItemStockInventoryRepository>.Instance,
            correlationContext,
            NullAuditTrailWriter.Instance);
    }

    private ItemStockInventoryExtendedRepository CreateItemStockInventoryExtendedRepository()
    {
        var correlationContext = new CorrelationContext();
        correlationContext.Set("seed", appId: "iis-wms-consumer-test", types: [KafkaEvents.InventoryAdjustedEventType]);

        return new ItemStockInventoryExtendedRepository(
            provider.GetRequiredService<ICosmosContainerFactory>(),
            NullLogger<ItemStockInventoryExtendedRepository>.Instance,
            correlationContext,
            NullAuditTrailWriter.Instance);
    }

    private static ItemStockInventory SeedAggregate(
        string id, string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark, int b2bAvailable) =>
        ItemStockInventory.Rehydrate(
            id, fulfilmentId, itemCode, countryOfOrigin, hallmark,
            b2bAvailable: b2bAvailable, b2cAvailable: 0, b2cOriginal: 0, b2cExtended: 0,
            b2cAllocated: 0, b2bAllocated: 0, b2cPrepared: 0, b2bPrepared: 0,
            internalHallmarkAllocated: 0, inTransit: 0, b2cThreshold: 0, isExtended: false, b2bUsedShare: 0,
            inspection: 0, psc: 0, isPosm: false, modifiedUtc: DateTime.UtcNow);

    private static string BuildAdjustedJson(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark, int quantity,
        InventoryEventStockState state, InventoryEventStockStatus status, string? referenceId,
        InventoryEventReasonCode reason = InventoryEventReasonCode.Adjustment,
        InventoryEventLocationType locationType = InventoryEventLocationType.Warehouse) =>
        JsonSerializer.Serialize(new
        {
            Channel = InventoryEventChannel.OtherStores,
            Adjustment = new
            {
                ReferenceId = referenceId,
                AdjustmentDate = DateTime.UtcNow,
                Entity = (string?)null,
                Type = InventoryEventChangeType.Unknown,
                State = new { State = state, Status = status },
                Location = new { Id = fulfilmentId, Type = locationType },
                Reason = reason,
                AdjustmentLines = new[]
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
            },
        });

    /// <summary>Polls <paramref name="check"/> until it returns a non-null result or the timeout elapses.</summary>
    private static async Task<T?> WaitForAsync<T>(Func<Task<T?>> check, TimeSpan? timeout = null)
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

        return default;
    }

    /// <summary>Polls <see cref="VirtualServiceBusBroker.Dispatched"/> until a message lands on <paramref name="queueName"/> or the timeout elapses.</summary>
    private async Task<(string QueueName, ServiceBusMessage Message)?> WaitForDispatchAsync(string queueName, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));

        while (DateTime.UtcNow < deadline)
        {
            var match = virtualServiceBusClient!.Broker.Dispatched.FirstOrDefault(entry => entry.QueueName == queueName);
            if (match.Message is not null)
            {
                return match;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }

        return null;
    }

    /// <summary>
    /// A dispatched message's body is a <see cref="ServiceBusRelayEnvelope"/> (built by
    /// <c>ServiceBusRelayPublisher.BuildMessageAsync</c>), not the raw payload - unwraps the
    /// envelope first, then deserializes its <see cref="ServiceBusRelayEnvelope.ReflexSchema"/> as
    /// <typeparamref name="T"/>, exactly as <see cref="ServiceBusConsumerHostedService{TMessage}"/>'s own
    /// two-step <c>TryDeserializeEnvelope</c>/<c>DeserializePayload</c> pipeline does for an inbound message.
    /// </summary>
    private static T? DeserializeRelayedBody<T>(ServiceBusMessage message)
    {
        var envelope = JsonSerializer.Deserialize<ServiceBusRelayEnvelope>(message.Body.ToString());
        return envelope!.ReflexSchema.Deserialize<T>();
    }
}
