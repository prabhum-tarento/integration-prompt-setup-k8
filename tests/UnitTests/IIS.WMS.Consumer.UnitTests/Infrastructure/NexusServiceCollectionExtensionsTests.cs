using IIS.WMS.Common.Messaging;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.NexusServices;
using IIS.WMS.Consumer.Infrastructure.Resilience;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Registration tests for <see cref="NexusServiceCollectionExtensions.AddNexusDeduplicationService"/> -
/// the typed <see cref="IDeduplicationService"/> client, its OAuth token handler, and the plain
/// token-request <see cref="HttpClient"/> (integration-resiliency.instructions.md §1).
/// </summary>
public class NexusServiceCollectionExtensionsTests
{
    private static IConfiguration BuildConfiguration(IDictionary<string, string?>? overrides = null)
    {
        var data = new Dictionary<string, string?>
        {
            ["Nexus:Deduplication:BaseUrl"] = "https://nexus.example.com",
            ["Nexus:Deduplication:OAuthEndpoint"] = "https://nexus.example.com/oauth/token",
            ["Nexus:Deduplication:ClientId"] = "client-id",
            ["Nexus:Deduplication:ClientSecret"] = "client-secret",
            ["Nexus:Deduplication:Scope"] = "dedupe",
            ["Application:AppName"] = "IIS.WMS.Consumer",
            ["Application:AppId"] = "wms-consumer",
        };

        if (overrides is not null)
        {
            foreach (var (key, value) in overrides)
            {
                data[key] = value;
            }
        }

        return new ConfigurationBuilder().AddInMemoryCollection(data).Build();
    }

    private static ServiceProvider BuildProvider(IConfiguration configuration)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddResiliencePipelines();
        services.Configure<ApplicationOptions>(configuration.GetSection(ApplicationOptions.SectionName));

        var result = services.AddNexusDeduplicationService(configuration);
        Assert.Same(services, result);

        return services.BuildServiceProvider();
    }

    [Fact(DisplayName = "AddNexusDeduplicationService registers IDeduplicationService backed by NexusDeduplicationService")]
    public void AddNexusDeduplicationService_ValidConfiguration_ResolvesTypedClient()
    {
        var provider = BuildProvider(BuildConfiguration());

        var deduplicationService = provider.GetRequiredService<IDeduplicationService>();

        Assert.IsType<NexusDeduplicationService>(deduplicationService);
    }

    [Fact(DisplayName = "The typed client's HttpClient carries the Nexus dedupe base address and App-Id header")]
    public void AddNexusDeduplicationService_ValidConfiguration_HttpClientConfigured()
    {
        var provider = BuildProvider(BuildConfiguration());
        var httpClientFactory = provider.GetRequiredService<IHttpClientFactory>();

        var client = httpClientFactory.CreateClient(nameof(IDeduplicationService));

        Assert.Equal(new Uri("https://nexus.example.com/nexus/deduper/api/dedupe"), client.BaseAddress);
        Assert.Equal("wms-consumer", client.DefaultRequestHeaders.GetValues(WellKnownHeaderNames.AppId).Single());
    }

    [Fact(DisplayName = "Resolving the typed client throws when Nexus:Deduplication:BaseUrl is missing")]
    public void AddNexusDeduplicationService_MissingBaseUrl_ThrowsOnResolve()
    {
        var provider = BuildProvider(BuildConfiguration(new Dictionary<string, string?> { ["Nexus:Deduplication:BaseUrl"] = null }));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IDeduplicationService>());
        Assert.Contains("Nexus:Deduplication:BaseUrl", exception.Message);
    }

    [Fact(DisplayName = "Resolving the typed client throws when Application:AppId is missing")]
    public void AddNexusDeduplicationService_MissingAppId_ThrowsOnResolve()
    {
        var provider = BuildProvider(BuildConfiguration(new Dictionary<string, string?> { ["Application:AppId"] = null }));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IDeduplicationService>());
        Assert.Contains("Application:AppId", exception.Message);
    }

    [Fact(DisplayName = "NexusAuthenticationHandler is registered transient - two resolutions yield distinct instances")]
    public void AddNexusDeduplicationService_Registered_NexusAuthenticationHandlerIsTransient()
    {
        var provider = BuildProvider(BuildConfiguration());

        var handler1 = provider.GetRequiredService<NexusAuthenticationHandler>();
        var handler2 = provider.GetRequiredService<NexusAuthenticationHandler>();

        Assert.NotSame(handler1, handler2);
    }
}
