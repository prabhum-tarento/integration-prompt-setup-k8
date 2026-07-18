using FluentValidation;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.Messaging;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging;
using IIS.WMS.Consumer.Infrastructure.Messaging.OrderArchiving;
using IIS.WMS.Consumer.Infrastructure.Resilience;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.AvroContracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="MessagingServiceCollectionExtensions.IsFunctionEnabled"/> - the
/// <c>Kafka:Functions</c> allow-list filter, mirroring an Azure Functions host's <c>functions</c>
/// startup filter rather than a per-consumer <c>Enabled</c> flag.
/// </summary>
public class MessagingServiceCollectionExtensionsTests
{
    [Fact(DisplayName = "A null filter allows every consumer")]
    public void IsFunctionEnabled_NullFilter_ReturnsTrue()
    {
        Assert.True(MessagingServiceCollectionExtensions.IsFunctionEnabled(null, KafkaEvents.InventoryAdjustedEventType));
    }

    [Fact(DisplayName = "An empty filter allows every consumer")]
    public void IsFunctionEnabled_EmptyFilter_ReturnsTrue()
    {
        Assert.True(MessagingServiceCollectionExtensions.IsFunctionEnabled([], KafkaEvents.InventoryAdjustedEventType));
    }

    [Fact(DisplayName = "A filter naming this consumer allows it")]
    public void IsFunctionEnabled_FilterContainsConsumer_ReturnsTrue()
    {
        var filter = new[] { KafkaEvents.InventoryAdjustedEventType };

        Assert.True(MessagingServiceCollectionExtensions.IsFunctionEnabled(filter, KafkaEvents.InventoryAdjustedEventType));
    }

    [Fact(DisplayName = "A filter naming only a different consumer excludes this one")]
    public void IsFunctionEnabled_FilterExcludesConsumer_ReturnsFalse()
    {
        var filter = new[] { KafkaEvents.InventoryAdjustedEventType };

        Assert.False(MessagingServiceCollectionExtensions.IsFunctionEnabled(filter, KafkaEvents.InventoryEventsConsumerKey));
    }

    [Fact(DisplayName = "The filter match is case-insensitive")]
    public void IsFunctionEnabled_FilterDifferentCase_ReturnsTrue()
    {
        var filter = new[] { "inventoryevents" };

        Assert.True(MessagingServiceCollectionExtensions.IsFunctionEnabled(filter, KafkaEvents.InventoryEventsConsumerKey));
    }

    /// <summary>
    /// Registration tests for <see cref="MessagingServiceCollectionExtensions.AddMessaging"/> itself -
    /// the Kafka/Service Bus options bindings (including the Kafka-level-fallback <c>PostConfigure</c>
    /// wiring), the shared relay-publisher/sender-cache singletons, and the <c>Kafka:KafkaEventFunctions</c>
    /// allow-list gating which Kafka consumers' hosted services and health checks actually get registered.
    /// </summary>
    private static IConfiguration BuildConfiguration(
        string[]? kafkaEventFunctions = null,
        bool includeServiceBusSection = true,
        bool includeBulkImportServiceBusSection = true,
        IDictionary<string, string?>? overrides = null)
    {
        var data = new Dictionary<string, string?>
        {
            ["Kafka:BootstrapServers"] = "kafka-level:9092",
            ["Kafka:SchemaRegistryUrl"] = "https://schema-registry.example.com",
            ["Kafka:ConsumerGroup"] = "iis-wms-consumer",
            ["Kafka:InventoryStateChanged:ConsumerGroup"] = "iis-wms-consumer",
            ["Kafka:BulkInventoryImport:ConsumerGroup"] = "iis-wms-consumer",
        };

        if (kafkaEventFunctions is not null)
        {
            for (var i = 0; i < kafkaEventFunctions.Length; i++)
            {
                data[$"Kafka:KafkaEventFunctions:{i}"] = kafkaEventFunctions[i];
            }
        }

        if (includeServiceBusSection)
        {
            data["ServiceBus:ConnectionString"] =
                "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123";
            data["ServiceBus:QueueName"] = "inventory-events";
        }

        if (includeBulkImportServiceBusSection)
        {
            data["ServiceBus:BulkInventoryImport:QueueName"] = "inventory-bulk-import";
        }

        if (overrides is not null)
        {
            foreach (var (key, value) in overrides)
            {
                data[key] = value;
            }
        }

        return new ConfigurationBuilder().AddInMemoryCollection(data).Build();
    }

    private static IServiceCollection BuildServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IFileStore>(
            BlobStorageServiceCollectionExtensions.HotTierKey, (_, _) => Substitute.For<IFileStore>());
        services.AddKeyedSingleton<IFileStore>(
            BlobStorageServiceCollectionExtensions.ColdTierKey, (_, _) => Substitute.For<IFileStore>());
        services.AddSingleton(Options.Create(new BlobStorageOptions()));
        services.AddSingleton(Substitute.For<IDeduplicationService>());
        services.AddSingleton(Substitute.For<IDynamicEventValidator>());
        services.AddSingleton(Substitute.For<IOrderArchiveWriter>());
        services.AddSingleton(Options.Create(new ApplicationOptions { AppName = "IIS.WMS.Consumer", AppId = "iis-wms-consumer" }));
        services.AddResiliencePipelines();
        return services;
    }

    private static IReadOnlyCollection<string> HealthCheckNames(IServiceProvider provider) =>
        provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations
            .Select(r => r.Name)
            .ToArray();

    [Fact(DisplayName = "AddMessaging returns the same collection for chaining")]
    public void AddMessaging_ValidConfiguration_ReturnsSameCollection()
    {
        var services = BuildServices();

        var result = services.AddMessaging(BuildConfiguration());

        Assert.Same(services, result);
    }

    [Fact(DisplayName = "AddMessaging throws when the ServiceBus section is missing")]
    public void AddMessaging_MissingServiceBusSection_Throws()
    {
        var services = BuildServices();

        var exception = Assert.Throws<InvalidOperationException>(
            () => services.AddMessaging(BuildConfiguration(includeServiceBusSection: false, includeBulkImportServiceBusSection: false)));

        Assert.Contains("ServiceBus:QueueName", exception.Message);
    }

    [Fact(DisplayName = "AddMessaging falls back to the default bulk-import queue name when that section is absent")]
    public void AddMessaging_MissingBulkImportSection_StillRegistersItsHealthCheck()
    {
        var services = BuildServices();
        services.AddMessaging(BuildConfiguration(includeBulkImportServiceBusSection: false));

        var provider = services.BuildServiceProvider();

        Assert.Contains("service-bus-bulk-import", HealthCheckNames(provider));
    }

    [Fact(DisplayName = "AddMessaging (no Kafka:KafkaEventFunctions filter) registers all three Kafka consumers and every health check")]
    public void AddMessaging_NoFilter_RegistersAllConsumersAndHealthChecks()
    {
        var services = BuildServices();
        services.AddMessaging(BuildConfiguration());

        Assert.Contains(services, d => d.ServiceType == typeof(KafkaConsumerHostedService));
        Assert.Contains(services, d => d.ServiceType == typeof(InventoryStateChangedConsumerHostedService));
        Assert.Contains(services, d => d.ServiceType == typeof(BulkInventoryImportConsumerHostedService));

        var provider = services.BuildServiceProvider();
        var hostedServices = provider.GetServices<IHostedService>().ToArray();
        Assert.Contains(hostedServices, hs => hs is KafkaConsumerHostedService);
        Assert.Contains(hostedServices, hs => hs is InventoryStateChangedConsumerHostedService);
        Assert.Contains(hostedServices, hs => hs is BulkInventoryImportConsumerHostedService);

        var healthCheckNames = HealthCheckNames(provider);
        Assert.Equal(
            new[]
            {
                "service-bus", "service-bus-bulk-import", "kafka-consumer",
                "inventory-state-changed-consumer", "inventory-adjusted-consumer", "bulk-import-consumer",
            },
            healthCheckNames);
    }

    [Fact(DisplayName = "A Kafka:KafkaEventFunctions filter naming only InventoryEvents registers just that consumer")]
    public void AddMessaging_FilterNamesOnlyInventoryEvents_RegistersOnlyThatConsumer()
    {
        var services = BuildServices();
        services.AddMessaging(BuildConfiguration([KafkaEvents.InventoryEventsConsumerKey]));

        Assert.Contains(services, d => d.ServiceType == typeof(KafkaConsumerHostedService));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(InventoryStateChangedConsumerHostedService));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(BulkInventoryImportConsumerHostedService));

        var provider = services.BuildServiceProvider();
        var healthCheckNames = HealthCheckNames(provider);

        Assert.Equal(new[] { "service-bus", "service-bus-bulk-import", "kafka-consumer" }, healthCheckNames);
    }

    [Fact(DisplayName = "A Kafka:KafkaEventFunctions filter naming only InventoryAdjusted registers InventoryStateChanged's hosted service with just the adjusted health check")]
    public void AddMessaging_FilterNamesOnlyInventoryAdjusted_RegistersOnlyThatHealthCheck()
    {
        var services = BuildServices();
        services.AddMessaging(BuildConfiguration([KafkaEvents.InventoryAdjustedEventType]));

        Assert.DoesNotContain(services, d => d.ServiceType == typeof(KafkaConsumerHostedService));
        Assert.Contains(services, d => d.ServiceType == typeof(InventoryStateChangedConsumerHostedService));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(BulkInventoryImportConsumerHostedService));

        var provider = services.BuildServiceProvider();
        var healthCheckNames = HealthCheckNames(provider);

        Assert.Contains("inventory-adjusted-consumer", healthCheckNames);
        Assert.DoesNotContain("inventory-state-changed-consumer", healthCheckNames);
    }

    [Fact(DisplayName = "A Kafka:KafkaEventFunctions filter naming none of the three consumers registers no Kafka consumers or their health checks")]
    public void AddMessaging_FilterExcludesEveryConsumer_RegistersNoKafkaConsumers()
    {
        var services = BuildServices();
        services.AddMessaging(BuildConfiguration(["SomeOtherFunction"]));

        Assert.DoesNotContain(services, d => d.ServiceType == typeof(KafkaConsumerHostedService));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(InventoryStateChangedConsumerHostedService));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(BulkInventoryImportConsumerHostedService));

        var provider = services.BuildServiceProvider();
        Assert.Equal(new[] { "service-bus", "service-bus-bulk-import" }, HealthCheckNames(provider));

        var hostedServices = provider.GetServices<IHostedService>().ToArray();
        Assert.DoesNotContain(hostedServices, hs => hs is KafkaConsumerHostedService);
        Assert.DoesNotContain(hostedServices, hs => hs is InventoryStateChangedConsumerHostedService);
        Assert.DoesNotContain(hostedServices, hs => hs is BulkInventoryImportConsumerHostedService);
    }

    [Fact(DisplayName = "AddMessaging registers the shared singletons every Kafka consumer depends on")]
    public void AddMessaging_ValidConfiguration_RegistersSharedSingletons()
    {
        var services = BuildServices();
        services.AddMessaging(BuildConfiguration());

        Assert.Contains(services, d => d.ServiceType == typeof(IValidator<BulkInventoryImportEvent>) && d.Lifetime == ServiceLifetime.Singleton);
        Assert.Contains(services, d => d.ServiceType == typeof(ISpecificRecordDeserializerFactory) && d.Lifetime == ServiceLifetime.Singleton);
        Assert.Contains(services, d => d.ServiceType == typeof(IServiceBusRelayPublisher) && d.Lifetime == ServiceLifetime.Singleton);
        Assert.Contains(services, d => d.ServiceType == typeof(IServiceBusSenderCacheService) && d.Lifetime == ServiceLifetime.Singleton);
        Assert.Contains(services, d => d.ServiceType == typeof(IServiceBusSenderCacheSource) && d.Lifetime == ServiceLifetime.Singleton);
        Assert.Contains(services, d => d.ServiceType == typeof(ConsumerRelayInfrastructure) && d.Lifetime == ServiceLifetime.Singleton);
    }

    [Fact(DisplayName = "AddMessaging's Kafka-level PostConfigure fills in Enabled/WorkerCount/etc. defaults when unset")]
    public void AddMessaging_KafkaLevelOptionsUnset_PostConfigureAppliesHardcodedDefaults()
    {
        var services = BuildServices();
        services.AddMessaging(BuildConfiguration());

        var options = services.BuildServiceProvider().GetRequiredService<IOptions<KafkaConsumerOptions>>().Value;

        Assert.True(options.Enabled);
        Assert.Equal(1, options.WorkerCount);
        Assert.Equal(1_000, options.ChannelCapacity);
        Assert.True(options.DeduplicationCheckEnabled);
        Assert.False(options.EnableAutoCommit);
        Assert.Equal(Confluent.Kafka.AutoOffsetReset.Earliest, options.AutoOffsetReset);
        Assert.Equal(ConsumerOptions.DefaultMaxServiceBusMessageSizeBytes, options.MaxServiceBusMessageSizeBytes);
    }

    [Fact(DisplayName = "AddMessaging's Kafka-level PostConfigure leaves an explicitly configured value alone")]
    public void AddMessaging_KafkaLevelOptionsSet_PostConfigureDoesNotOverride()
    {
        var services = BuildServices();
        services.AddMessaging(BuildConfiguration(overrides: new Dictionary<string, string?>
        {
            ["Kafka:Enabled"] = "false",
            ["Kafka:WorkerCount"] = "5",
            ["Kafka:EnableAutoCommit"] = "true",
            ["Kafka:AutoOffsetReset"] = "Latest",
        }));

        var options = services.BuildServiceProvider().GetRequiredService<IOptions<KafkaConsumerOptions>>().Value;

        Assert.False(options.Enabled);
        Assert.Equal(5, options.WorkerCount);
        Assert.True(options.EnableAutoCommit);
        Assert.Equal(Confluent.Kafka.AutoOffsetReset.Latest, options.AutoOffsetReset);
    }

    [Fact(DisplayName = "Event-level Kafka options fall back to the Kafka-level BootstrapServers/SchemaRegistryUrl when unset")]
    public void AddMessaging_EventLevelOptionsUnset_FallBackToKafkaLevel()
    {
        var services = BuildServices();
        services.AddMessaging(BuildConfiguration(overrides: new Dictionary<string, string?>
        {
            ["Kafka:SchemaRegistryUrl"] = "https://schema-registry.example.com",
        }));

        var provider = services.BuildServiceProvider();

        var stateChangedOptions = provider.GetRequiredService<IOptions<InventoryStateChangedConsumerOptions>>().Value;
        var bulkImportOptions = provider.GetRequiredService<IOptions<BulkInventoryImportConsumerOptions>>().Value;

        Assert.Equal("kafka-level:9092", stateChangedOptions.BootstrapServers);
        Assert.Equal("https://schema-registry.example.com", stateChangedOptions.SchemaRegistryUrl);
        Assert.Equal("kafka-level:9092", bulkImportOptions.BootstrapServers);
        Assert.Equal("https://schema-registry.example.com", bulkImportOptions.SchemaRegistryUrl);
    }

    [Fact(DisplayName = "An explicit event-level BootstrapServers wins over the Kafka-level fallback")]
    public void AddMessaging_EventLevelOptionsSet_EventLevelWinsOverKafkaLevel()
    {
        var services = BuildServices();
        services.AddMessaging(BuildConfiguration(overrides: new Dictionary<string, string?>
        {
            ["Kafka:InventoryStateChanged:BootstrapServers"] = "event-level:9092",
        }));

        var provider = services.BuildServiceProvider();
        var stateChangedOptions = provider.GetRequiredService<IOptions<InventoryStateChangedConsumerOptions>>().Value;

        Assert.Equal("event-level:9092", stateChangedOptions.BootstrapServers);
    }
}
