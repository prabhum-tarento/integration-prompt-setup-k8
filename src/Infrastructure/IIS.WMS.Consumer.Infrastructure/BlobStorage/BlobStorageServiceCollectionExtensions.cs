using Azure.Identity;
using Azure.Storage.Blobs;
using IIS.WMS.Consumer.Application.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.BlobStorage;

/// <summary>Blob Storage client/file-store registration per integration-resiliency.instructions.md §5.</summary>
public static class BlobStorageServiceCollectionExtensions
{
    /// <summary>Registers the singleton <see cref="BlobServiceClient"/> and the scoped <see cref="IFileStore"/>.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>BlobStorage</c> section.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddBlobStorage(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<BlobStorageOptions>(configuration.GetSection(BlobStorageOptions.SectionName));

        services.AddSingleton(sp =>
        {
            var config = configuration.GetSection(BlobStorageOptions.SectionName).Get<BlobStorageOptions>()
                ?? throw new InvalidOperationException($"Missing '{BlobStorageOptions.SectionName}' configuration section.");
            var env = sp.GetRequiredService<IHostEnvironment>();
            var logger = sp.GetRequiredService<ILogger<BlobServiceClient>>();

            // Local dev: Azurite connection string via user-secrets. Every other environment
            // authenticates with DefaultAzureCredential (AKS Workload Identity), same pattern as
            // Cosmos DB (cosmos-db.instructions.md §1).
            if (env.IsDevelopment())
            {
                logger.LogInformation("Configuring Blob Storage client using the local Azurite connection string.");

                return new BlobServiceClient(configuration["BlobStorage:ConnectionString"]);
            }

            logger.LogInformation(
                "Configuring Blob Storage client for {AccountUri} using DefaultAzureCredential.", config.AccountUri);

            return new BlobServiceClient(new Uri(config.AccountUri), new DefaultAzureCredential());
        });

        services.AddScoped<IFileStore, BlobFileStore>();

        return services;
    }
}
