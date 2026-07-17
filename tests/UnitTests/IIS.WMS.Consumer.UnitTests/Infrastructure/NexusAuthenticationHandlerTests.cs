using System.Net;
using System.Net.Http.Json;
using IIS.WMS.Consumer.Infrastructure.NexusServices;
using IIS.WMS.Common.Resilience;
using IIS.WMS.Consumer.UnitTests.Infrastructure.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Polly;
using Polly.Registry;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="NexusAuthenticationHandler"/> - attaches a bearer token acquired
/// from the Nexus OAuth endpoint, and caches it across calls instead of requesting a fresh one every
/// time (integration-resiliency.instructions.md §1).
/// </summary>
public class NexusAuthenticationHandlerTests
{
    [Fact(DisplayName = "Attaches the access token from the OAuth endpoint as a Bearer header")]
    public async Task SendAsync_FirstCall_AttachesBearerToken()
    {
        var oauthHandler = new StubHttpMessageHandler
        {
            Respond = _ => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(new { access_token = "token-1", expires_in = 3600 }),
            },
        };
        var downstreamHandler = new StubHttpMessageHandler();
        var handler = CreateHandler(oauthHandler, downstreamHandler);

        using var invoker = new HttpMessageInvoker(handler);
        await invoker.SendAsync(new HttpRequestMessage(HttpMethod.Post, "https://nexus.example/dedup"), CancellationToken.None);

        var downstreamRequest = Assert.Single(downstreamHandler.Requests);
        Assert.Equal("Bearer", downstreamRequest.Headers.Authorization!.Scheme);
        Assert.Equal("token-1", downstreamRequest.Headers.Authorization!.Parameter);
    }

    [Fact(DisplayName = "A cached, unexpired token is reused instead of requesting a new one per call")]
    public async Task SendAsync_CalledTwiceWithinExpiry_RequestsTokenOnlyOnce()
    {
        var oauthHandler = new StubHttpMessageHandler
        {
            Respond = _ => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(new { access_token = "token-1", expires_in = 3600 }),
            },
        };
        var downstreamHandler = new StubHttpMessageHandler();
        var handler = CreateHandler(oauthHandler, downstreamHandler);

        using var invoker = new HttpMessageInvoker(handler);
        await invoker.SendAsync(new HttpRequestMessage(HttpMethod.Post, "https://nexus.example/dedup"), CancellationToken.None);
        await invoker.SendAsync(new HttpRequestMessage(HttpMethod.Post, "https://nexus.example/dedup"), CancellationToken.None);

        Assert.Single(oauthHandler.Requests);
        Assert.Equal(2, downstreamHandler.Requests.Count);
    }

    private static NexusAuthenticationHandler CreateHandler(StubHttpMessageHandler oauthHandler, HttpMessageHandler downstreamHandler)
    {
        var services = new ServiceCollection();
        services.AddResiliencePipeline<string, HttpResponseMessage>(ResiliencePipelines.OutboundHttp, _ => { });
        services.AddHttpClient(NexusServiceCollectionExtensions.NexusOAuthHttpClientName)
            .ConfigurePrimaryHttpMessageHandler(() => oauthHandler);

        var provider = services.BuildServiceProvider();

        var options = Options.Create(new NexusDeduplicationOptions
        {
            BaseUrl = "https://nexus.example/dedup",
            OAuthEndpoint = "https://nexus.example/oauth/token",
            ClientId = "client-id",
            ClientSecret = "client-secret",
            Scope = "dedup.readwrite",
        });

        return new NexusAuthenticationHandler(
            provider.GetRequiredService<IHttpClientFactory>(),
            options,
            provider.GetRequiredService<ResiliencePipelineProvider<string>>(),
            Substitute.For<ILogger<NexusAuthenticationHandler>>())
        {
            InnerHandler = downstreamHandler,
        };
    }
}
