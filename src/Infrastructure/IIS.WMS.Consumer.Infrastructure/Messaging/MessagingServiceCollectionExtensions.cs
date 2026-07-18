using Confluent.Kafka;
using FluentValidation;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.Messaging;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.AvroContracts;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.Validators;
using IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus.Handlers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace IIS.WMS.Consumer.Infrastructure.Messaging;

/// <summary>
/// Registers the three Kafka → Service Bus relays (the JSON-contract
/// <see cref="Kafka.KafkaConsumerHostedService"/>, the Avro/Schema-Registry
/// <see cref="Kafka.InventoryStateChangedConsumerHostedService"/>, and the high-volume
/// <see cref="Kafka.BulkInventoryImportConsumerHostedService"/>, all built on the shared
/// <see cref="Kafka.KafkaConsumerHostedServiceBase"/>) and both Service Bus → Cosmos DB consumers (the
/// session-enabled <see cref="ServiceBus.InventoryStateChangedServiceBusHostedService"/> and the non-session
/// <see cref="ServiceBus.BulkImportServiceBusConsumerHostedService"/>)
/// (integration-resiliency.instructions.md). Each event type a Kafka consumer registers gets its own
/// <see cref="ConsumerHealthState"/>, built internally by that consumer (see
/// <see cref="KafkaConsumerHostedServiceBase.RegisterSchemaHandlers"/>) rather than injected here - a stall on
/// one event type must not be masked by another still polling. Each consumer as a whole can still be
/// turned off independently via its own <c>Enabled</c> configuration key,
/// checked by <see cref="KafkaConsumerHostedServiceBase"/> at startup - adding a further schema
/// consumer means adding its options/hosted-service/health-check trio here, not touching the
/// shared base class - the Schema Registry wiring itself is shared via the singleton
/// <see cref="Kafka.ISpecificRecordDeserializerFactory"/> registered below, so that consumer would
/// not need to duplicate it either. Each event-level consumer's <c>Enabled</c>,
/// <c>BootstrapServers</c>, and <c>SchemaRegistryUrl</c> also fall back to the top-level
/// <c>Kafka</c> section's values when left unset (<see cref="Kafka.ConsumerOptions.ApplyKafkaLevelDefaults"/>),
/// and <see cref="Kafka.KafkaConsumerOptions.KafkaEventFunctions"/> gates which consumers get registered at
/// all here, independently of their own <c>Enabled</c> flag. All hosted services are registered on the same host as the Api for
/// this skeleton; kubernetes-deployment-best-practices.instructions.md's target topology is separate
/// Deployments (Api, Kafka consumers, Service Bus consumers) each with their own image -
/// splitting these <see cref="Microsoft.Extensions.Hosting.BackgroundService"/> registrations into
/// their own minimal host projects is a follow-up, not done here.
/// </summary>
public static class MessagingServiceCollectionExtensions
{
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
        // Shared publisher every Kafka consumer (and, per integration-resiliency.instructions.md,
        // any future Service Bus-side handler/service) relays through - registered once as a singleton
        // rather than per consumer, so its ServiceBusSender cache is truly one sender per queue across
        // the whole process, not one per caller. See ServiceBusRelayPublisher's own remarks. Shared
        // between the Kafka and Service Bus ingestion paths below, so it lives here rather than in
        // either one.
        services.AddSingleton<ServiceBusRelayPublisher>();
        services.AddSingleton<IServiceBusRelayPublisher>(sp => sp.GetRequiredService<ServiceBusRelayPublisher>());

        // Fans out to every IServiceBusSenderCacheSource registered below - just the shared publisher
        // above now, since it owns every cached ServiceBusSender in this process. Backs the admin
        // endpoint that lists/clears them. Single-process scope only - see
        // IServiceBusSenderCacheService's own remarks.
        services.AddSingleton<IServiceBusSenderCacheService, ServiceBusSenderCacheService>();
        services.AddSingleton<IServiceBusSenderCacheSource>(sp => sp.GetRequiredService<ServiceBusRelayPublisher>());

        // Service Bus consumers first, Kafka consumers second - matches the original single-method
        // registration order (service-bus/service-bus-bulk-import health checks before the Kafka ones),
        // which MessagingServiceCollectionExtensionsTests asserts via exact-order HealthCheckNames checks.
        services.AddServiceBusConsumers(configuration);
        services.AddKafkaConsumers(configuration);

        return services;
    }

    /// <summary>Registers the three Kafka consumers (options, deserializer factory, relay facade, hosted services, and health checks).</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>Kafka</c> and <c>Kafka:InventoryStateChanged</c> sections.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddKafkaConsumers(this IServiceCollection services, IConfiguration configuration)
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
            options.MaxServiceBusMessageSizeBytes ??= ConsumerOptions.DefaultMaxServiceBusMessageSizeBytes;
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

        services.AddSingleton<ISpecificRecordDeserializerFactory, SpecificRecordDeserializerFactory>();

        // Facade bundling the six dependencies every Kafka consumer needs and which never vary
        // between them - see ConsumerRelayInfrastructure's own doc comment for why this exists and
        // what's deliberately excluded from it.
        services.AddSingleton<ConsumerRelayInfrastructure>();

        // Read once, directly off configuration - Functions is a startup-time registration filter,
        // not a per-request setting, so it doesn't need the options pattern's change-tracking.
        var functionsFilter = configuration.GetSection(KafkaConsumerOptions.SectionName).Get<KafkaConsumerOptions>()?.KafkaEventFunctions;
        var allowAll = (functionsFilter?.Length ?? 0) == 0;

        RegisterInventoryEventsConsumer(services, functionsFilter, allowAll);
        RegisterInventoryStateChangedConsumer(services, functionsFilter, allowAll);
        RegisterBulkImportConsumer(services, functionsFilter, allowAll);

        return services;
    }

    /// <param name="services">The service collection to register against.</param>
    /// <param name="functionsFilter">The configured Kafka consumer allow-list, or <see langword="null"/>/empty for "no filter."</param>
    /// <param name="allowAll">Whether <paramref name="functionsFilter"/> is empty (every consumer allowed).</param>
    private static void RegisterInventoryEventsConsumer(IServiceCollection services, string[]? functionsFilter, bool allowAll)
    {
        if (allowAll || IsFunctionEnabled(functionsFilter, KafkaEvents.InventoryEventsConsumerKey))
        {
            AddKafkaConsumer<KafkaConsumerHostedService>(
                services,
                (KafkaConsumerHostedServiceBase.DefaultEventType, "kafka-consumer", "InventoryEvents Kafka consumer health"));
        }
    }

    /// <summary>
    /// Gated by InventoryStateChangedEventType alone, even though this consumer also relays
    /// InventoryAdjusted off the same topic/consumer group - see
    /// InventoryStateChangedConsumerHostedService's own remarks on why one poll loop covers both
    /// event types. Both still get their own entry below, matching the two keys that consumer's own
    /// RegisterSchemaHandlers call registers, so each gets its own ConsumerHealthCheck.
    /// </summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="functionsFilter">The configured Kafka consumer allow-list, or <see langword="null"/>/empty for "no filter."</param>
    /// <param name="allowAll">Whether <paramref name="functionsFilter"/> is empty (every consumer allowed).</param>
    private static void RegisterInventoryStateChangedConsumer(IServiceCollection services, string[]? functionsFilter, bool allowAll)
    {
        if (!allowAll && !IsFunctionEnabled(functionsFilter, KafkaEvents.InventoryStateChangedEventType)
            && !IsFunctionEnabled(functionsFilter, KafkaEvents.InventoryAdjustedEventType))
        {
            return;
        }

        var healthChecks = new List<(string EventType, string HealthCheckName, string DisplayName)>();
        if (IsFunctionEnabled(functionsFilter, KafkaEvents.InventoryStateChangedEventType))
        {
            healthChecks.Add((KafkaEvents.InventoryStateChangedEventType, "inventory-state-changed-consumer", "InventoryStateChanged Kafka consumer health"));
        }

        if (IsFunctionEnabled(functionsFilter, KafkaEvents.InventoryAdjustedEventType))
        {
            healthChecks.Add((KafkaEvents.InventoryAdjustedEventType, "inventory-adjusted-consumer", "InventoryAdjusted Kafka consumer health"));
        }

        AddKafkaConsumer<InventoryStateChangedConsumerHostedService>(
            services,
            healthChecks.ToArray());
    }

    /// <param name="services">The service collection to register against.</param>
    /// <param name="functionsFilter">The configured Kafka consumer allow-list, or <see langword="null"/>/empty for "no filter."</param>
    /// <param name="allowAll">Whether <paramref name="functionsFilter"/> is empty (every consumer allowed).</param>
    private static void RegisterBulkImportConsumer(IServiceCollection services, string[]? functionsFilter, bool allowAll)
    {
        if (allowAll || IsFunctionEnabled(functionsFilter, KafkaEvents.BulkInventoryImportConsumerKey))
        {
            AddKafkaConsumer<BulkInventoryImportConsumerHostedService>(
                services,
                (KafkaConsumerHostedServiceBase.DefaultEventType, "bulk-import-consumer", "BulkInventoryImport Kafka consumer health"));
        }
    }

    /// <summary>Registers the Service Bus consuming infrastructure, both hosted services, and their queue health checks.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>ServiceBus</c> section.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddServiceBusConsumers(this IServiceCollection services, IConfiguration configuration)
    {
        // Shared Service Bus consuming base setup (client/admin client, options binding,
        // ICorrelationContext, ServiceBusHealthStateRegistry) - common to every project that consumes
        // off a Service Bus queue, see ServiceBusServiceCollectionExtensions's own remarks.
        services.AddServiceBusConsumerInfrastructure(configuration);

        // Facade bundling the plumbing dependencies every ServiceBusConsumerHostedService<TMessage>
        // needs and which never vary between queues - see ServiceBusConsumerDependencies's own doc
        // comment for why this exists and what's deliberately excluded from it (the Kafka-side mirror
        // is ConsumerRelayInfrastructure, registered the same way above in AddKafkaConsumers).
        services.AddSingleton<ServiceBusConsumerDependencies>();

        // No parent above ServiceBus level to fall back to, so this is the one place "unset" resolves
        // to a hardcoded default rather than a fallback lookup - preserves today's session-processor
        // behavior for a queue that doesn't override these at its own level. Mirrors
        // AddKafkaConsumers' own Kafka-level PostConfigure bottom-out above.
        services.PostConfigure<ServiceBusConsumerOptions>(options =>
        {
            options.MaxConcurrentSessions ??= 8;
            options.MaxConcurrentCallsPerSession ??= 1;
        });

        // Read once, directly off configuration - mirrors AddKafkaConsumers' own functionsFilter read
        // below, same "gates registration, independent of any per-consumer enable flag" semantics, just
        // for the two Service Bus consumers instead of the three Kafka ones.
        var functionsFilter = configuration.GetSection(ServiceBusConsumerOptions.SectionName).Get<ServiceBusConsumerOptions>()?.ServiceBusEventFunctions;
        var allowAll = (functionsFilter?.Length ?? 0) == 0;

        RegisterInventoryEventsServiceBusConsumer(services, configuration, functionsFilter, allowAll);
        RegisterBulkImportServiceBusConsumer(services, configuration, functionsFilter, allowAll);

        return services;
    }

    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>ServiceBus</c> section.</param>
    /// <param name="functionsFilter">The configured Service Bus consumer allow-list, or <see langword="null"/>/empty for "no filter."</param>
    /// <param name="allowAll">Whether <paramref name="functionsFilter"/> is empty (every consumer allowed).</param>
    private static void RegisterInventoryEventsServiceBusConsumer(
        IServiceCollection services, IConfiguration configuration, string[]? functionsFilter, bool allowAll)
    {
        if (!allowAll && !IsFunctionEnabled(functionsFilter, InventoryEventsServiceBusKey))
        {
            return;
        }

        services.Configure<InventoryStateChangedServiceBusConsumerOptions>(
            configuration.GetSection(InventoryStateChangedServiceBusConsumerOptions.SectionName));

        // Queue level first, ServiceBus level as fallback (InventoryStateChangedServiceBusConsumerOptions.ApplyServiceBusLevelDefaults).
        // Resolving IOptions<ServiceBusConsumerOptions> here is what runs its PostConfigure above
        // first, so MaxConcurrentSessions/MaxConcurrentCallsPerSession are never null by this point.
        services.AddOptions<InventoryStateChangedServiceBusConsumerOptions>()
            .PostConfigure<IOptions<ServiceBusConsumerOptions>>(
                (eventOptions, serviceBusOptions) => eventOptions.ApplyServiceBusLevelDefaults(serviceBusOptions.Value));

        // ServiceBusHealthCheck takes a queue name directly (not IOptions<ServiceBusConsumerOptions>)
        // so both queues can share the one check type - see its own doc comment. Registered here,
        // where `configuration` is in scope, rather than InfrastructureServiceCollectionExtensions,
        // mirroring how each Kafka consumer's health check is registered alongside its hosted service.
        var serviceBusQueueName = configuration.GetSection(ServiceBusConsumerOptions.SectionName).Get<ServiceBusConsumerOptions>()?.QueueName
            ?? throw new InvalidOperationException($"Missing '{ServiceBusConsumerOptions.SectionName}:{nameof(ServiceBusConsumerOptions.QueueName)}'.");

        // ActivatorUtilities.CreateInstance is needed here (rather than a plain AddHostedService<T>())
        // because queueName is a plain constructor string with no attribute DI can resolve it from -
        // every other constructor parameter is filled from the container as usual.
        services.AddHostedService(sp => ActivatorUtilities.CreateInstance<InventoryStateChangedServiceBusHostedService>(
            sp, serviceBusQueueName));
        services.AddScoped<IInventoryStateChangedHandler, InventoryStateChangedHandler>();

        services.AddServiceBusQueueHealthCheck("service-bus", serviceBusQueueName, "service-bus-consumer");
    }

    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>ServiceBus</c> section.</param>
    /// <param name="functionsFilter">The configured Service Bus consumer allow-list, or <see langword="null"/>/empty for "no filter."</param>
    /// <param name="allowAll">Whether <paramref name="functionsFilter"/> is empty (every consumer allowed).</param>
    private static void RegisterBulkImportServiceBusConsumer(
        IServiceCollection services, IConfiguration configuration, string[]? functionsFilter, bool allowAll)
    {
        if (!allowAll && !IsFunctionEnabled(functionsFilter, BulkInventoryImportServiceBusKey))
        {
            return;
        }

        services.AddHostedService<BulkImportServiceBusConsumerHostedService>();

        var bulkImportQueueName = configuration.GetSection(BulkImportServiceBusConsumerOptions.SectionName).Get<BulkImportServiceBusConsumerOptions>()?.QueueName
            ?? new BulkImportServiceBusConsumerOptions().QueueName;
        services.AddServiceBusQueueHealthCheck("service-bus-bulk-import", bulkImportQueueName, "service-bus-consumer");
    }

    /// <summary>Whether <paramref name="consumerKey"/> should start, per an allow-list's "run only the named ones" semantics - shared by <see cref="Kafka.KafkaConsumerOptions.KafkaEventFunctions"/> and <see cref="ServiceBusConsumerOptions.ServiceBusEventFunctions"/>.</summary>
    /// <param name="functionsFilter">The configured allow-list, or <see langword="null"/>/empty for "no filter."</param>
    /// <param name="consumerKey">
    /// The candidate consumer's allow-list key - one of <see cref="KafkaEvents.InventoryEventsConsumerKey"/>,
    /// <see cref="KafkaEvents.InventoryStateChangedEventType"/>, or <see cref="KafkaEvents.BulkInventoryImportConsumerKey"/>
    /// for <c>Kafka:KafkaEventFunctions</c>; or <see cref="InventoryEventsServiceBusKey"/>/<see cref="BulkInventoryImportServiceBusKey"/>
    /// for <c>ServiceBus:ServiceBusEventFunctions</c>.
    /// </param>
    public static bool IsFunctionEnabled(IReadOnlyCollection<string>? functionsFilter, string consumerKey) =>
        functionsFilter is null || functionsFilter.Count == 0
            || functionsFilter.Contains(consumerKey, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Registers one Kafka consumer's <see cref="BackgroundService"/> - also resolvable by its own
    /// concrete <typeparamref name="THostedService"/> type (not just <see cref="IHostedService"/>), so
    /// the health checks below can reach the same running instance - plus one
    /// <see cref="ConsumerHealthCheck"/> per <paramref name="eventTypeHealthChecks"/> entry. Each
    /// check's <see cref="ConsumerHealthState"/> is resolved off that consumer instance via
    /// <see cref="KafkaConsumerHostedServiceBase.GetHealthState"/> lazily, at check-execution time rather than
    /// here: <see cref="KafkaConsumerHostedServiceBase.RegisterSchemaHandlers"/> (which builds those states)
    /// doesn't run until <typeparamref name="THostedService"/>'s own constructor does, so there is
    /// nothing yet to bind a health check to when this method runs at startup - by the time anything
    /// actually calls <c>/health/ready</c>, the host has already constructed every hosted service.
    /// </summary>
    /// <typeparam name="THostedService">The concrete <see cref="KafkaConsumerHostedServiceBase"/> subclass to register.</typeparam>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="eventTypeHealthChecks">
    /// One entry per event type this consumer passes to its own <see cref="KafkaConsumerHostedServiceBase.RegisterSchemaHandlers"/>
    /// call (<see cref="KafkaConsumerHostedServiceBase.DefaultEventType"/> for a consumer that registers exactly
    /// one, regardless of the Kafka <c>Type</c> header) paired with the health check name to register
    /// it under and the display name used in its log lines/description. Always tagged
    /// <c>kafka-consumer</c> - every Kafka consumer shares that pod in the target topology, see
    /// kubernetes-deployment-best-practices.instructions.md.
    /// </param>
    private static void AddKafkaConsumer<THostedService>(
        IServiceCollection services,
        params (string EventType, string HealthCheckName, string DisplayName)[] eventTypeHealthChecks)
        where THostedService : KafkaConsumerHostedServiceBase
    {
        services.AddSingleton<THostedService>();
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<THostedService>());

        var healthChecksBuilder = services.AddHealthChecks();

        foreach (var (eventType, healthCheckName, displayName) in eventTypeHealthChecks)
        {
            healthChecksBuilder.Add(new HealthCheckRegistration(
                healthCheckName,
                sp => new ConsumerHealthCheck(
                    sp.GetRequiredService<THostedService>().GetHealthState(eventType),
                    displayName,
                    sp.GetRequiredService<ILogger<ConsumerHealthCheck>>()),
                failureStatus: null,
                tags: ["kafka-consumer"]));
        }
    }
}
