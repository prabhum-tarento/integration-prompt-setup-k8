using IIS.WMS.Consumer.Infrastructure.BlobStorage;
using IIS.WMS.Consumer.Infrastructure.Messaging;
using IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Resilience;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace IIS.WMS.Consumer.Infrastructure.DependencyInjection;

/// <summary>Composition-root entry point for the Infrastructure layer - called once from <c>Program.cs</c>.</summary>
public static class InfrastructureServiceCollectionExtensions
{
    /// <summary>Registers resilience pipelines, Cosmos DB, Blob Storage, Kafka/Service Bus messaging, and all health checks.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, passed through to each sub-registration.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddResiliencePipelines();
        services.AddCosmosDb(configuration);
        services.AddBlobStorage(configuration);

        // Each Kafka consumer's health check is registered alongside its hosted service inside
        // AddMessaging (see MessagingServiceCollectionExtensions.AddKafkaConsumer) rather than
        // here, since each needs its own ConsumerHealthState instance passed in at registration
        // time. Every check across this method and AddMessaging is still tagged for the Pod it
        // would run on in the target 3-Deployment topology
        // (kubernetes-deployment-best-practices.instructions.md) - a Pod's readiness probe can
        // only observe its own process, so Program.cs maps a separate endpoint per tag rather
        // than one /health/ready that blends all three (aspnet-rest-apis.instructions.md,
        // integration-resiliency.instructions.md §8), even though this skeleton currently hosts
        // all three workloads in one process.
        services.AddMessaging(configuration);

        services.AddHealthChecks()
            .AddCheck<CosmosHealthCheck>("cosmos-db", tags: ["api"])
            .AddCheck<ServiceBusHealthCheck>("service-bus", tags: ["service-bus-consumer"]);

        return services;
    }
}
