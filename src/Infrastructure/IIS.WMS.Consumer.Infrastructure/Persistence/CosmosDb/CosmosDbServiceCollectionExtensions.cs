using Azure.Identity;
using IIS.WMS.Consumer.Application.BulkInventoryImport;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>Cosmos client/container/repository registration per cosmos-db.instructions.md §2 and §12.</summary>
public static class CosmosDbServiceCollectionExtensions
{
    /// <summary>
    /// Keyed-service key for the bulk-import <see cref="CosmosClient"/>/<see cref="ICosmosContainerFactory"/>
    /// pair - a separate client from the default one, with <c>AllowBulkExecution = true</c>. Bulk mode
    /// optimizes for throughput over per-call latency, which is right for the high-volume bulk-import
    /// consumer (integration-resiliency.instructions.md §1) and wrong for the latency-sensitive
    /// reserve/allocate path the default client serves - see the Microsoft Cosmos DB bulk-executor
    /// guidance this follows.
    /// </summary>
    public const string BulkCosmosClientKey = "bulk";

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

            var options = BuildCosmosClientOptions(env, allowBulkExecution: false);

            logger.LogInformation(
                "Configuring Cosmos client for {AccountEndpoint} using the local emulator key.", config.AccountEndpoint);

            return new CosmosClient(config.AccountEndpoint, config.EmulatorKey, options);
        });

        // Container factory: its own singleton, resolving named containers once from the client - this
        // is what every repository injects, not CosmosClient/Container directly. Each repository declares
        // its own container name (§1) rather than reading a single shared CosmosDb:ContainerName. Never
        // call CreateDatabaseIfNotExistsAsync/CreateContainerIfNotExistsAsync here - provisioning is a
        // Bicep/Terraform concern, not application startup (cosmos-db.instructions.md §2).
        services.AddSingleton<ICosmosContainerFactory, CosmosContainerFactory>();

        services.AddScoped<IInventoryEventRepository, InventoryEventRepository>();
        services.AddScoped<IOrderArchiveRepository, OrderArchiveRepository>();
        services.AddScoped<IItemStockInventoryRepository, ItemStockInventoryRepository>();
        services.AddScoped<IFulfilmentLevelSegmentationRepository, FulfilmentLevelSegmentationRepository>();
        services.AddScoped<IItemLevelSegmentationRepository, ItemLevelSegmentationRepository>();
        services.AddScoped<IMessageArchiveRepository, MessageArchiveRepository>();

        // Bulk-import client: same account/database, but AllowBulkExecution = true and connection
        // settings tuned for throughput (Microsoft's documented bulk-executor guidance), so the
        // bulk-import consumer's high message volume doesn't fight the default client's
        // latency-sensitive settings above. A separate keyed CosmosClient, not a shared one with
        // bulk mode toggled on, because AllowBulkExecution is a client-wide setting - it would
        // silently degrade the reserve/allocate path's latency if applied there too.
        services.AddKeyedSingleton<CosmosClient>(BulkCosmosClientKey, (sp, _) =>
        {
            var config = configuration.GetSection(CosmosDbOptions.SectionName).Get<CosmosDbOptions>()
                ?? throw new InvalidOperationException($"Missing '{CosmosDbOptions.SectionName}' configuration section.");
            var env = sp.GetRequiredService<IHostEnvironment>();
            var logger = sp.GetRequiredService<ILogger<CosmosClient>>();

            var options = BuildCosmosClientOptions(env, allowBulkExecution: true);

            logger.LogInformation(
                "Configuring bulk-import Cosmos client for {AccountEndpoint} using the local emulator key.", config.AccountEndpoint);

            return new CosmosClient(config.AccountEndpoint, config.EmulatorKey, options);
        });

        services.AddKeyedSingleton<ICosmosContainerFactory>(BulkCosmosClientKey, (sp, key) =>
            new CosmosContainerFactory(
                sp.GetRequiredKeyedService<CosmosClient>(key!), sp.GetRequiredService<IOptions<CosmosDbOptions>>()));

        services.AddScoped<IBulkInventoryImportRepository, InventoryBulkImportItemRepository>();

        return services;
    }

    /// <summary>
    /// Builds the <see cref="CosmosClientOptions"/> shared by the default and bulk-import clients,
    /// differing only in <see cref="CosmosClientOptions.AllowBulkExecution"/> and - for a non-dev bulk
    /// client only - the Direct-mode TCP tuning below (Microsoft's documented bulk-executor guidance).
    /// </summary>
    /// <param name="env">Used to select the dev-emulator cert-bypass branch below.</param>
    /// <param name="allowBulkExecution">Whether this is the bulk-import client.</param>
    private static CosmosClientOptions BuildCosmosClientOptions(IHostEnvironment env, bool allowBulkExecution)
    {
        var options = new CosmosClientOptions
        {
            ConsistencyLevel = ConsistencyLevel.Session,
            AllowBulkExecution = allowBulkExecution,
            MaxRetryAttemptsOnRateLimitedRequests = 9,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
            SerializerOptions = new CosmosSerializationOptions
            {
                PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase,
            },
        };

        if (env.IsDevelopment())
        {
            // Local Cosmos DB Emulator (Podman Linux vNext, scripts/local-emulators) serves a
            // self-signed cert that isn't chained to a trusted root, and setup-podman-emulators.bat
            // recreates the container - with a fresh cert - on every run, so trusting it via the OS
            // cert store would go stale immediately. HttpClientFactory/the custom validation callback
            // is only honored under Gateway mode, not Direct.
            options.ConnectionMode = ConnectionMode.Gateway;
            options.HttpClientFactory = () => new HttpClient(new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (_, _, _, _) => true,
            });
        }
        else if (allowBulkExecution)
        {
            // MaxRequestsPerTcpConnection/MaxTcpConnectionsPerEndpoint are Direct-mode-only settings -
            // the Cosmos SDK's CosmosClientOptions validation throws if they're set while
            // ConnectionMode is Gateway, so they're only applied in this non-dev, bulk-only branch,
            // which is the only place Direct mode is ever used.
            options.MaxRequestsPerTcpConnection = 30;
            options.MaxTcpConnectionsPerEndpoint = 10;
        }

        return options;
    }
}
