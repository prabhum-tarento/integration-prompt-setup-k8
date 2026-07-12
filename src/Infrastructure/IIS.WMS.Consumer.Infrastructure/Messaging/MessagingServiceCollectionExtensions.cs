using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Confluent.Kafka;
using FluentValidation;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.AvroContracts;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.Validators;
using IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging;

/// <summary>
/// Registers the three Kafka → Service Bus relays (the JSON-contract
/// <see cref="Kafka.KafkaConsumerHostedService"/>, the Avro/Schema-Registry
/// <see cref="Kafka.InventoryStateChangedConsumerHostedService"/>, and the high-volume
/// <see cref="Kafka.BulkInventoryImportConsumerHostedService"/>, all built on the shared
/// <see cref="Kafka.ConsumerHostedService"/>) and both Service Bus → Cosmos DB consumers (the
/// session-enabled <see cref="ServiceBus.ServiceBusConsumerHostedService"/> and the non-session
/// <see cref="ServiceBus.BulkImportServiceBusConsumerHostedService"/>)
/// (integration-resiliency.instructions.md). Each Kafka consumer gets its own keyed
/// <see cref="ConsumerHealthState"/> instance (a stall in one must not be masked by another still
/// polling) and can be turned off independently via its own <c>Enabled</c> configuration key,
/// checked by <see cref="ConsumerHostedService"/> at startup - adding a further schema
/// consumer means adding its options/hosted-service/health-check trio here, not touching the
/// shared base class - the Schema Registry wiring itself is shared via the singleton
/// <see cref="Kafka.ISpecificRecordDeserializerFactory"/> registered below, so that consumer would
/// not need to duplicate it either. Each event-level consumer's <c>Enabled</c>,
/// <c>BootstrapServers</c>, and <c>SchemaRegistryUrl</c> also fall back to the top-level
/// <c>Kafka</c> section's values when left unset (<see cref="Kafka.ConsumerOptions.ApplyKafkaLevelDefaults"/>),
/// and <see cref="Kafka.KafkaConsumerOptions.Functions"/> gates which consumers get registered at
/// all here, independently of their own <c>Enabled</c> flag. All hosted services are registered on the same host as the Api for
/// this skeleton; kubernetes-deployment-best-practices.instructions.md's target topology is separate
/// Deployments (Api, Kafka consumers, Service Bus consumers) each with their own image -
/// splitting these <see cref="Microsoft.Extensions.Hosting.BackgroundService"/> registrations into
/// their own minimal host projects is a follow-up, not done here.
/// </summary>
public static class MessagingServiceCollectionExtensions
{
    /// <summary>Keyed-service key for the JSON-contract consumer's <see cref="ConsumerHealthState"/>.</summary>
    public const string InventoryEventsConsumerKey = "InventoryEvents";

    /// <summary>Keyed-service key for the Avro/Schema-Registry consumer's <see cref="ConsumerHealthState"/>.</summary>
    public const string InventoryStateChangedConsumerKey = "InventoryStateChanged";

    /// <summary>Keyed-service key for the high-volume bulk-import consumer's <see cref="ConsumerHealthState"/>.</summary>
    public const string BulkInventoryImportConsumerKey = "BulkInventoryImport";

    /// <summary>Keyed-service key for the session-enabled queue consumer's <see cref="ServiceBus.ServiceBusHealthState"/>.</summary>
    public const string InventoryEventsServiceBusKey = "InventoryEvents";

    /// <summary>Keyed-service key for the non-session bulk-import queue consumer's <see cref="ServiceBus.ServiceBusHealthState"/>.</summary>
    public const string BulkInventoryImportServiceBusKey = "BulkInventoryImport";

    /// <summary>Registers the Service Bus clients, all three hosted services, and their health checks.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>Kafka</c>, <c>Kafka:InventoryStateChanged</c>, and <c>ServiceBus</c> sections.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<KafkaConsumerOptions>(configuration.GetSection(KafkaConsumerOptions.SectionName));

        // No parent above Kafka level to fall back to, so this is the one place "unset" resolves to
        // a hardcoded default rather than a fallback lookup - preserves the always-on-unless-said-
        // otherwise behavior every event-level consumer's own fallback (below) ultimately bottoms out
        // at, for all six settings, not just Enabled. EnableAutoCommit/AutoOffsetReset specifically
        // must be pinned here, not left null: Confluent.Kafka's own defaults (true/Latest) are the
        // opposite of what this service's manual-commit architecture requires
        // (ConsumerOptions.EnableAutoCommit's own remarks) - leaving them to "fall back to nothing"
        // the way BootstrapServers/SchemaRegistryUrl legitimately can would be a correctness bug.
        services.PostConfigure<KafkaConsumerOptions>(options =>
        {
            options.Enabled ??= true;
            options.WorkerCount ??= 1;
            options.ChannelCapacity ??= 1_000;
            options.DeduplicationCheckEnabled ??= true;
            options.EnableAutoCommit ??= false;
            options.AutoOffsetReset ??= AutoOffsetReset.Earliest;
        });

        services.Configure<InventoryStateChangedConsumerOptions>(
            configuration.GetSection(InventoryStateChangedConsumerOptions.SectionName));

        // Event level first, Kafka level as fallback (ConsumerOptions.ApplyKafkaLevelDefaults) for
        // Enabled/BootstrapServers/SchemaRegistryUrl. Resolving IOptions<KafkaConsumerOptions> here
        // is what runs its PostConfigure above first, so Enabled is never null by this point.
        services.AddOptions<InventoryStateChangedConsumerOptions>()
            .PostConfigure<IOptions<KafkaConsumerOptions>>(
                (eventOptions, kafkaOptions) => eventOptions.ApplyKafkaLevelDefaults(kafkaOptions.Value));

        services.Configure<BulkInventoryImportConsumerOptions>(
            configuration.GetSection(BulkInventoryImportConsumerOptions.SectionName));

        services.AddOptions<BulkInventoryImportConsumerOptions>()
            .PostConfigure<IOptions<KafkaConsumerOptions>>(
                (eventOptions, kafkaOptions) => eventOptions.ApplyKafkaLevelDefaults(kafkaOptions.Value));

        // Stateless, like every other FluentValidation validator - but registered explicitly as a
        // singleton rather than via AddValidatorsFromAssembly (which defaults to Scoped): this
        // validator is injected directly into the singleton BulkInventoryImportConsumerHostedService
        // (a BackgroundService), and a Scoped registration would throw the same way a Scoped IFileStore
        // would (see BlobStorageServiceCollectionExtensions's comment on the same issue).
        services.AddSingleton<IValidator<BulkInventoryImportEvent>, BulkInventoryImportEventValidator>();

        services.Configure<ServiceBusConsumerOptions>(configuration.GetSection(ServiceBusConsumerOptions.SectionName));
        services.Configure<BulkImportServiceBusConsumerOptions>(
            configuration.GetSection(BulkImportServiceBusConsumerOptions.SectionName));

        services.AddSingleton(sp =>
        {
            var env = sp.GetRequiredService<IHostEnvironment>();
            var logger = sp.GetRequiredService<ILogger<ServiceBusClient>>();
            var options = configuration.GetSection(ServiceBusConsumerOptions.SectionName).Get<ServiceBusConsumerOptions>()
                ?? throw new InvalidOperationException(
                    $"Missing '{ServiceBusConsumerOptions.SectionName}' configuration section.");

            logger.LogInformation("Configuring Service Bus client using connection string.");
            return new ServiceBusClient(options.ConnectionString);
        });

        services.AddSingleton(sp =>
        {
            var env = sp.GetRequiredService<IHostEnvironment>();
            var logger = sp.GetRequiredService<ILogger<ServiceBusAdministrationClient>>();
            var options = configuration.GetSection(ServiceBusConsumerOptions.SectionName).Get<ServiceBusConsumerOptions>()
                ?? throw new InvalidOperationException(
                    $"Missing '{ServiceBusConsumerOptions.SectionName}' configuration section.");

            logger.LogInformation("Configuring Service Bus Admin client using connection string.");
            return new ServiceBusAdministrationClient(options.ConnectionString);
        });

        // Keyed, not a single shared singleton - the bulk-import consumer below gets its own
        // ServiceBusHealthState instance, and a plain (unkeyed) second AddSingleton<ServiceBusHealthState>()
        // would silently collide (last registration wins for unkeyed resolution).
        if (false)
        {
            services.AddKeyedSingleton<ServiceBusHealthState>(InventoryEventsServiceBusKey);
            services.AddHostedService<ServiceBusConsumerHostedService>();

            services.AddKeyedSingleton<ServiceBusHealthState>(BulkInventoryImportServiceBusKey);
            services.AddHostedService<BulkImportServiceBusConsumerHostedService>();
        }

        // ServiceBusHealthCheck takes a queue name directly (not IOptions<ServiceBusConsumerOptions>)
        // so both queues can share the one check type - see its own doc comment. Registered here,
        // where `configuration` is in scope, rather than InfrastructureServiceCollectionExtensions,
        // mirroring how each Kafka consumer's health check is registered alongside its hosted service.
        var serviceBusQueueName = configuration.GetSection(ServiceBusConsumerOptions.SectionName).Get<ServiceBusConsumerOptions>()?.QueueName
            ?? throw new InvalidOperationException($"Missing '{ServiceBusConsumerOptions.SectionName}:{nameof(ServiceBusConsumerOptions.QueueName)}'.");
        var bulkImportQueueName = configuration.GetSection(BulkImportServiceBusConsumerOptions.SectionName).Get<BulkImportServiceBusConsumerOptions>()?.QueueName
            ?? new BulkImportServiceBusConsumerOptions().QueueName;

        services.AddHealthChecks()
            .AddTypeActivatedCheck<ServiceBusHealthCheck>("service-bus", failureStatus: null, tags: ["service-bus-consumer"], args: [serviceBusQueueName])
            .AddTypeActivatedCheck<ServiceBusHealthCheck>("service-bus-bulk-import", failureStatus: null, tags: ["service-bus-consumer"], args: [bulkImportQueueName]);

        services.AddSingleton<ISpecificRecordDeserializerFactory, SpecificRecordDeserializerFactory>();

        // Facade bundling the six dependencies every Kafka consumer needs and which never vary
        // between them - see ConsumerRelayInfrastructure's own doc comment for why this exists and
        // what's deliberately excluded from it.
        services.AddSingleton<ConsumerRelayInfrastructure>();

        // Read once, directly off configuration - Functions is a startup-time registration filter,
        // not a per-request setting, so it doesn't need the options pattern's change-tracking.
        var functionsFilter = configuration.GetSection(KafkaConsumerOptions.SectionName).Get<KafkaConsumerOptions>()?.Functions;
        var allowAll = (functionsFilter?.Length ?? 0) == 0;
        if (allowAll || IsFunctionEnabled(functionsFilter, InventoryEventsConsumerKey))
        {
            AddKafkaConsumer<KafkaConsumerHostedService>(
                services, InventoryEventsConsumerKey, "kafka-consumer", "InventoryEvents Kafka consumer health");
        }

        if (allowAll || IsFunctionEnabled(functionsFilter, InventoryStateChangedConsumerKey))
        {
            AddKafkaConsumer<InventoryStateChangedConsumerHostedService>(
                services, InventoryStateChangedConsumerKey, "inventory-state-changed-consumer", "InventoryStateChanged Kafka consumer health");
        }

        if (allowAll || IsFunctionEnabled(functionsFilter, BulkInventoryImportConsumerKey))
        {
            AddKafkaConsumer<BulkInventoryImportConsumerHostedService>(
                services, BulkInventoryImportConsumerKey, "bulk-import-consumer", "BulkInventoryImport Kafka consumer health");
        }

        return services;
    }

    /// <summary>Whether <paramref name="consumerKey"/> should start, per <see cref="KafkaConsumerOptions.Functions"/>'s allow-list semantics.</summary>
    /// <param name="functionsFilter">The configured allow-list, or <see langword="null"/>/empty for "no filter."</param>
    /// <param name="consumerKey">The candidate consumer's keyed-service key (<see cref="InventoryEventsConsumerKey"/> or <see cref="InventoryStateChangedConsumerKey"/>).</param>
    public static bool IsFunctionEnabled(IReadOnlyCollection<string>? functionsFilter, string consumerKey) =>
        functionsFilter is null || functionsFilter.Count == 0
            || functionsFilter.Contains(consumerKey, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Registers one Kafka consumer's keyed <see cref="ConsumerHealthState"/>, its
    /// <see cref="BackgroundService"/>, and a matching <see cref="ConsumerHealthCheck"/> bound to
    /// that same state instance - the trio every <see cref="ConsumerHostedService"/>
    /// subclass needs, factored out so adding another consumer doesn't repeat this wiring.
    /// </summary>
    /// <typeparam name="THostedService">The concrete <see cref="ConsumerHostedService"/> subclass to register.</typeparam>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="healthStateKey">Keyed-service key this consumer's hosted service resolves its <see cref="ConsumerHealthState"/> by (via <c>[FromKeyedServices]</c>).</param>
    /// <param name="healthCheckName">Name the health check is registered under. Always tagged <c>kafka-consumer</c> - both Kafka consumers share that pod in the target topology, see kubernetes-deployment-best-practices.instructions.md.</param>
    /// <param name="displayName">Human-readable name used in this consumer's log lines and health check description.</param>
    private static void AddKafkaConsumer<THostedService>(
        IServiceCollection services, string healthStateKey, string healthCheckName, string displayName)
        where THostedService : BackgroundService
    {
        var healthState = new ConsumerHealthState();

        services.AddKeyedSingleton(healthStateKey, healthState);
        services.AddHostedService<THostedService>();

        services.AddHealthChecks().AddTypeActivatedCheck<ConsumerHealthCheck>(
            healthCheckName, failureStatus: null, tags: ["kafka-consumer"], args: [healthState, displayName]);
    }
}
