using Azure.Identity;
using Azure.Storage.Blobs;
using IIS.WMS.Consumer.Application.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly.Registry;

namespace IIS.WMS.Consumer.Infrastructure.BlobStorage;

/// <summary>
/// Blob Storage client/file-store registration per integration-resiliency.instructions.md §5. Hot and
/// cold tiers are backed by separate Storage accounts, so this registers a keyed
/// <see cref="BlobServiceClient"/>/<see cref="IFileStore"/> pair per tier (<see cref="HotTierKey"/>,
/// <see cref="ColdTierKey"/>) rather than one shared instance - a caller writing to both tiers (e.g.
/// <see cref="Messaging.Kafka.ConsumerHostedService"/>) resolves each via
/// <c>[FromKeyedServices]</c>.
/// </summary>
public static class BlobStorageServiceCollectionExtensions
{
    /// <summary>Keyed-service key for the hot-tier <see cref="BlobServiceClient"/>/<see cref="IFileStore"/>.</summary>
    public const string HotTierKey = "hot";

    /// <summary>Keyed-service key for the cold-tier <see cref="BlobServiceClient"/>/<see cref="IFileStore"/>.</summary>
    public const string ColdTierKey = "cold";

    /// <summary>Registers a keyed <see cref="BlobServiceClient"/> and <see cref="IFileStore"/> per tier (<see cref="HotTierKey"/>, <see cref="ColdTierKey"/>).</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>BlobStorage</c> section.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddBlobStorage(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<BlobStorageOptions>(configuration.GetSection(BlobStorageOptions.SectionName));

        AddTier(services, configuration, HotTierKey, options => options.Hot);
        AddTier(services, configuration, ColdTierKey, options => options.Cold);

        return services;
    }

    /// <summary>Registers one tier's keyed <see cref="BlobServiceClient"/> and <see cref="IFileStore"/>.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>BlobStorage</c> section.</param>
    /// <param name="tierKey"><see cref="HotTierKey"/> or <see cref="ColdTierKey"/> - the keyed-service key this tier's client/file store is registered under.</param>
    /// <param name="selectAccount">Picks this tier's <see cref="BlobStorageAccountOptions"/> (<see cref="BlobStorageOptions.Hot"/> or <see cref="BlobStorageOptions.Cold"/>) off the bound options.</param>
    private static void AddTier(
        IServiceCollection services,
        IConfiguration configuration,
        string tierKey,
        Func<BlobStorageOptions, BlobStorageAccountOptions> selectAccount)
    {
        services.AddKeyedSingleton<BlobServiceClient>(tierKey, (sp, _) =>
        {
            var config = configuration.GetSection(BlobStorageOptions.SectionName).Get<BlobStorageOptions>()
                ?? throw new InvalidOperationException($"Missing '{BlobStorageOptions.SectionName}' configuration section.");
            var account = selectAccount(config);
            var env = sp.GetRequiredService<IHostEnvironment>();
            var logger = sp.GetRequiredService<ILogger<BlobServiceClient>>();

            logger.LogInformation(
                "Configuring {Tier}-tier Blob Storage client.", tierKey);

            return new BlobServiceClient(account.AccountUri);
        });

        // Singleton, not Scoped - BlobFileStore is stateless (its own dependencies, BlobServiceClient
        // and ResiliencePipelineProvider<string>, are already singletons), and it needs to be safe to
        // inject directly into the singleton ConsumerHostedService (a BackgroundService) for
        // the cold/hot-tier audit logging added in integration-resiliency.instructions.md §1 - a
        // Scoped registration would throw ("cannot consume scoped service from singleton") the moment
        // a Kafka consumer resolved it without going through a manually created DI scope per message.
        services.AddKeyedSingleton<IFileStore>(tierKey, (sp, key) =>
            new BlobFileStore(
                sp.GetRequiredKeyedService<BlobServiceClient>(key!),
                sp.GetRequiredService<ResiliencePipelineProvider<string>>(),
                sp.GetRequiredService<ILogger<BlobFileStore>>()));
    }
}
