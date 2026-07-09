using Azure.Identity;
using IIS.WMS.Consumer.Application.InventoryEvents;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>Cosmos client/container/repository registration per cosmos-db.instructions.md §2 and §12.</summary>
public static class CosmosDbServiceCollectionExtensions
{
    /// <summary>Registers the singleton <see cref="CosmosClient"/>/<see cref="ICosmosContainerFactory"/> and the scoped <see cref="IInventoryEventRepository"/>.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>CosmosDb</c> section.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddCosmosDb(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<CosmosDbOptions>(configuration.GetSection(CosmosDbOptions.SectionName));

        // CosmosClient: singleton, created once - never per-request or inside a controller.
        services.AddSingleton(sp =>
        {
            var config = configuration.GetSection(CosmosDbOptions.SectionName).Get<CosmosDbOptions>()
                ?? throw new InvalidOperationException($"Missing '{CosmosDbOptions.SectionName}' configuration section.");
            var env = sp.GetRequiredService<IHostEnvironment>();
            var logger = sp.GetRequiredService<ILogger<CosmosClient>>();

            var options = new CosmosClientOptions
            {
                ConsistencyLevel = ConsistencyLevel.Session,
                MaxRetryAttemptsOnRateLimitedRequests = 9,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
                SerializerOptions = new CosmosSerializationOptions
                {
                    PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase,
                },
            };

            // Local dev only: the Cosmos DB Emulator's well-known fixed key, from user-secrets.
            // Every other environment authenticates with DefaultAzureCredential (AKS Workload
            // Identity) - there is no key configuration entry once you leave IsDevelopment().
            if (env.IsDevelopment())
            {
                logger.LogInformation(
                    "Configuring Cosmos client for {AccountEndpoint} using the local emulator key.", config.AccountEndpoint);

                return new CosmosClient(config.AccountEndpoint, config.EmulatorKey, options);
            }

            logger.LogInformation(
                "Configuring Cosmos client for {AccountEndpoint} using DefaultAzureCredential.", config.AccountEndpoint);

            return new CosmosClient(config.AccountEndpoint, new DefaultAzureCredential(), options);
        });

        // Container factory: its own singleton, resolving named containers once from the client - this
        // is what every repository injects, not CosmosClient/Container directly. Each repository declares
        // its own container name (§1) rather than reading a single shared CosmosDb:ContainerName. Never
        // call CreateDatabaseIfNotExistsAsync/CreateContainerIfNotExistsAsync here - provisioning is a
        // Bicep/Terraform concern, not application startup (cosmos-db.instructions.md §2).
        services.AddSingleton<ICosmosContainerFactory, CosmosContainerFactory>();

        services.AddScoped<IInventoryEventRepository, InventoryEventRepository>();

        return services;
    }
}
