using IIS.WMS.Consumer.Application.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.NexusServices;

/// <summary>Registers the Nexus deduplication API client used by the Kafka consumer's dedup check (integration-resiliency.instructions.md §1).</summary>
public static class NexusServiceCollectionExtensions
{
    /// <summary>
    /// Name of the plain (unauthenticated) named <see cref="HttpClient"/> <see cref="NexusAuthenticationHandler"/>
    /// uses to request its own OAuth token - deliberately separate from the typed dedup client so the
    /// token request itself doesn't loop back through the auth handler.
    /// </summary>
    public const string NexusOAuthHttpClientName = "nexus-oauth";

    /// <summary>Registers <see cref="IDeduplicationService"/> (backed by <see cref="NexusDeduplicationService"/>) and its OAuth token handler.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>Nexus:Deduplication</c> section.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddNexusDeduplicationService(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<NexusDeduplicationOptions>(configuration.GetSection(NexusDeduplicationOptions.SectionName));

        services.AddHttpClient(NexusOAuthHttpClientName);

        // Transient per standard IHttpMessageHandlerFactory guidance for DelegatingHandlers - the
        // handler pipeline itself is pooled/rotated by IHttpClientFactory, not the handler instance.
        services.AddTransient<NexusAuthenticationHandler>();

        services.AddHttpClient<IDeduplicationService, NexusDeduplicationService>((sp, client) =>
        {
            var options = sp.GetRequiredService<IOptions<NexusDeduplicationOptions>>().Value;
            var applicationOptions = sp.GetRequiredService<IOptions<ApplicationOptions>>().Value;

            var clientBaseAddress = new Uri(
                options.BaseUrl
                    ?? throw new InvalidOperationException(
                        $"Missing '{NexusDeduplicationOptions.SectionName}:{nameof(NexusDeduplicationOptions.BaseUrl)}' configuration."));

            client.BaseAddress = new Uri($"{clientBaseAddress.GetLeftPart(UriPartial.Authority)}/nexus/deduper/api/dedupe");

            client.DefaultRequestHeaders.Add(
                Messaging.Kafka.KafkaHeaderNames.AppId,
                applicationOptions.AppId
                    ?? throw new InvalidOperationException(
                        $"Missing '{ApplicationOptions.SectionName}:{nameof(ApplicationOptions.AppId)}' configuration."));
        }).AddHttpMessageHandler<NexusAuthenticationHandler>();

        return services;
    }
}
