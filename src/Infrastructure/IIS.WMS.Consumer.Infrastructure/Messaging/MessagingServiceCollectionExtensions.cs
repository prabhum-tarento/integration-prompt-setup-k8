using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Messaging;

/// <summary>
/// Registers the two Kafka → Service Bus relays (the JSON-contract
/// <see cref="Kafka.KafkaConsumerHostedService"/> and the Avro/Schema-Registry
/// <see cref="Kafka.InventoryStateChangedConsumerHostedService"/>, both built on the shared
/// <see cref="Kafka.ConsumerHostedService{TValue}"/>) and the Service Bus → Cosmos DB consumer
/// (integration-resiliency.instructions.md). Each Kafka consumer gets its own keyed
/// <see cref="ConsumerHealthState"/> instance (a stall in one must not be masked by the other still
/// polling) and can be turned off independently via its own <c>Enabled</c> configuration key,
/// checked by <see cref="ConsumerHostedService{TValue}"/> at startup - adding a third schema
/// consumer means adding its options/hosted-service/health-check trio here, not touching the
/// shared base classes. All three hosted services are registered on the same host as the Api for
/// this skeleton; kubernetes-deployment-best-practices.instructions.md's target topology is three
/// separate Deployments (Api, Kafka consumer, Service Bus consumer) each with their own image -
/// splitting these <see cref="Microsoft.Extensions.Hosting.BackgroundService"/> registrations into
/// their own minimal host projects is a follow-up, not done here.
/// </summary>
public static class MessagingServiceCollectionExtensions
{
    /// <summary>Keyed-service key for the JSON-contract consumer's <see cref="ConsumerHealthState"/>.</summary>
    public const string InventoryEventsConsumerKey = "InventoryEvents";

    /// <summary>Keyed-service key for the Avro/Schema-Registry consumer's <see cref="ConsumerHealthState"/>.</summary>
    public const string InventoryStateChangedConsumerKey = "InventoryStateChanged";

    /// <summary>Registers the Service Bus clients, all three hosted services, and their health checks.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>Kafka</c>, <c>Kafka:InventoryStateChanged</c>, and <c>ServiceBus</c> sections.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<KafkaConsumerOptions>(configuration.GetSection(KafkaConsumerOptions.SectionName));

        services.Configure<InventoryStateChangedConsumerOptions>(
            configuration.GetSection(InventoryStateChangedConsumerOptions.SectionName));

        services.Configure<ServiceBusConsumerOptions>(configuration.GetSection(ServiceBusConsumerOptions.SectionName));

        services.AddSingleton(sp =>
        {
            var env = sp.GetRequiredService<IHostEnvironment>();
            var logger = sp.GetRequiredService<ILogger<ServiceBusClient>>();
            var options = configuration.GetSection(ServiceBusConsumerOptions.SectionName).Get<ServiceBusConsumerOptions>()
                ?? throw new InvalidOperationException(
                    $"Missing '{ServiceBusConsumerOptions.SectionName}' configuration section.");

            // Local dev: the Service Bus emulator via connection string from user-secrets. Every
            // other environment authenticates with DefaultAzureCredential (AKS Workload Identity).
            if (env.IsDevelopment())
            {
                logger.LogInformation("Configuring Service Bus client using the local emulator connection string.");

                return new ServiceBusClient(configuration["ServiceBus:ConnectionString"]);
            }

            logger.LogInformation(
                "Configuring Service Bus client for {Namespace} using DefaultAzureCredential.",
                options.FullyQualifiedNamespace);

            return new ServiceBusClient(options.FullyQualifiedNamespace, new DefaultAzureCredential());
        });

        services.AddSingleton(sp =>
        {
            var env = sp.GetRequiredService<IHostEnvironment>();
            var options = configuration.GetSection(ServiceBusConsumerOptions.SectionName).Get<ServiceBusConsumerOptions>()
                ?? throw new InvalidOperationException(
                    $"Missing '{ServiceBusConsumerOptions.SectionName}' configuration section.");

            return env.IsDevelopment()
                ? new ServiceBusAdministrationClient(configuration["ServiceBus:ConnectionString"])
                : new ServiceBusAdministrationClient(options.FullyQualifiedNamespace, new DefaultAzureCredential());
        });

        services.AddSingleton<ServiceBusHealthState>();
        services.AddHostedService<ServiceBusConsumerHostedService>();

        AddKafkaConsumer<KafkaConsumerHostedService>(
            services, InventoryEventsConsumerKey, "kafka-consumer", "InventoryEvents Kafka consumer health");

        AddKafkaConsumer<InventoryStateChangedConsumerHostedService>(
            services, InventoryStateChangedConsumerKey, "inventory-state-changed-consumer", "InventoryStateChanged Kafka consumer health");

        return services;
    }

    /// <summary>
    /// Registers one Kafka consumer's keyed <see cref="ConsumerHealthState"/>, its
    /// <see cref="BackgroundService"/>, and a matching <see cref="ConsumerHealthCheck"/> bound to
    /// that same state instance - the trio every <see cref="ConsumerHostedService{TValue}"/>
    /// subclass needs, factored out so adding another consumer doesn't repeat this wiring.
    /// </summary>
    /// <typeparam name="THostedService">The concrete <see cref="ConsumerHostedService{TValue}"/> subclass to register.</typeparam>
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
