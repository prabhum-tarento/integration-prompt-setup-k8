using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using IIS.WMS.Common.Correlation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Common.Messaging.ServiceBus;

/// <summary>
/// Base setup shared by every project that consumes messages off an Azure Service Bus queue - the
/// Consumer project's session-enabled inventory-events queue and non-session bulk-import queue today,
/// and the planned Producer project's own Service Bus consumption tomorrow. Registers the shared
/// <see cref="ServiceBusClient"/>/<see cref="ServiceBusAdministrationClient"/>, binds
/// <see cref="ServiceBusConsumerOptions"/>/<see cref="BulkImportServiceBusConsumerOptions"/>, and
/// registers <see cref="ICorrelationContext"/> - what's deliberately left out (the concrete hosted
/// services, their keyed <see cref="ServiceBusHealthState"/> instances, and health-check
/// registrations) stays with whichever project owns the business logic that reads from the queue,
/// since that's where the queue names and consumer-specific wiring are known.
/// </summary>
public static class ServiceBusServiceCollectionExtensions
{
    /// <summary>
    /// Registers the shared <see cref="ServiceBusClient"/>/<see cref="ServiceBusAdministrationClient"/>,
    /// binds the Service Bus options sections, and registers <see cref="ICorrelationContext"/>.
    /// </summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>ServiceBus</c>/<c>ServiceBus:BulkInventoryImport</c> sections.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddServiceBusConsumerInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddScoped<ICorrelationContext, CorrelationContext>();

        services.Configure<ServiceBusConsumerOptions>(configuration.GetSection(ServiceBusConsumerOptions.SectionName));
        services.Configure<BulkImportServiceBusConsumerOptions>(
            configuration.GetSection(BulkImportServiceBusConsumerOptions.SectionName));

        services.AddSingleton(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<ServiceBusClient>>();
            var options = configuration.GetSection(ServiceBusConsumerOptions.SectionName).Get<ServiceBusConsumerOptions>()
                ?? throw new InvalidOperationException(
                    $"Missing '{ServiceBusConsumerOptions.SectionName}' configuration section.");

            logger.LogInformation(
                "Configuring Service Bus client using connection string. TransportType: {TransportType}, RetryMode: {RetryMode}, MaxRetries: {MaxRetries}",
                options.TransportType, options.Retry.Mode, options.Retry.MaxRetries);

            // Registered once as a singleton and reused for the lifetime of the app - ServiceBusClient
            // (and every ServiceBusSender created from it) is expensive to construct and explicitly
            // documented as safe to share, per Microsoft's Service Bus client-lifetime guidance.
            var clientOptions = new ServiceBusClientOptions
            {
                TransportType = options.TransportType,
                RetryOptions = options.Retry,
            };

            return new ServiceBusClient(options.ConnectionString, clientOptions);
        });

        services.AddSingleton(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<ServiceBusAdministrationClient>>();
            var options = configuration.GetSection(ServiceBusConsumerOptions.SectionName).Get<ServiceBusConsumerOptions>()
                ?? throw new InvalidOperationException(
                    $"Missing '{ServiceBusConsumerOptions.SectionName}' configuration section.");

            logger.LogInformation("Configuring Service Bus Admin client using connection string.");
            return new ServiceBusAdministrationClient(options.ConnectionString);
        });

        return services;
    }

    /// <summary>
    /// Registers a <see cref="ServiceBusHealthCheck"/> for one queue - a thin wrapper so callers don't
    /// need to reach for <c>AddTypeActivatedCheck</c> directly for this common case.
    /// </summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="name">The health check's registered name.</param>
    /// <param name="queueName">The queue this check verifies reachability of.</param>
    /// <param name="tags">Tags controlling which <c>/health/ready</c> endpoint surfaces this check.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddServiceBusQueueHealthCheck(
        this IServiceCollection services, string name, string queueName, params string[] tags)
    {
        services.AddHealthChecks()
            .AddTypeActivatedCheck<ServiceBusHealthCheck>(name, failureStatus: null, tags: tags, args: [queueName]);

        return services;
    }
}
