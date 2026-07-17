using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using IIS.WMS.Common.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly.Registry;

namespace IIS.WMS.Consumer.Infrastructure.NexusServices;

/// <summary>
/// Attaches an OAuth2 client-credentials bearer token to every outgoing request on the Nexus
/// deduplication typed <see cref="HttpClient"/> (see <see cref="NexusServiceCollectionExtensions"/>).
/// Caches the token in memory and refreshes it shortly before expiry rather than acquiring a fresh
/// token per call - at this consumer's throughput (up to <c>ConsumerOptions.WorkerCount</c> concurrent
/// workers), a per-call token request would otherwise hammer the Nexus OAuth endpoint.
/// </summary>
/// <remarks>
/// Requests the token via a distinct, unauthenticated named <see cref="HttpClient"/>
/// (<see cref="NexusServiceCollectionExtensions.NexusOAuthHttpClientName"/>) rather than the client
/// this handler is itself attached to - reusing the same client would recurse back into this handler.
/// </remarks>
public sealed class NexusAuthenticationHandler(
    IHttpClientFactory httpClientFactory,
    IOptions<NexusDeduplicationOptions> options,
    ResiliencePipelineProvider<string> pipelineProvider,
    ILogger<NexusAuthenticationHandler> logger)
    : DelegatingHandler
{
    // Refresh a little before the token actually expires so a request in flight never races a
    // just-expired token.
    private static readonly TimeSpan ExpiryBuffer = TimeSpan.FromSeconds(60);

    private readonly SemaphoreSlim refreshLock = new(1, 1);
    private volatile CachedToken? cachedToken;

    /// <inheritdoc />
    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var accessToken = await GetAccessTokenAsync(cancellationToken);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

        return await base.SendAsync(request, cancellationToken);
    }

    private async Task<string> GetAccessTokenAsync(CancellationToken cancellationToken)
    {
        var current = cachedToken;

        if (current is not null && current.ExpiresAtUtc > DateTimeOffset.UtcNow + ExpiryBuffer)
        {
            return current.AccessToken;
        }

        await refreshLock.WaitAsync(cancellationToken);
        try
        {
            // Re-check inside the lock - another worker may have already refreshed it while this
            // one was waiting.
            current = cachedToken;
            if (current is not null && current.ExpiresAtUtc > DateTimeOffset.UtcNow + ExpiryBuffer)
            {
                return current.AccessToken;
            }

            var fresh = await RequestNewTokenAsync(cancellationToken);
            cachedToken = fresh;
            return fresh.AccessToken;
        }
        finally
        {
            refreshLock.Release();
        }
    }

    private async Task<CachedToken> RequestNewTokenAsync(CancellationToken cancellationToken)
    {
        var settings = options.Value;
        var httpClient = httpClientFactory.CreateClient(NexusServiceCollectionExtensions.NexusOAuthHttpClientName);
        var pipeline = pipelineProvider.GetPipeline<HttpResponseMessage>(ResiliencePipelines.OutboundHttp);

        logger.LogDebug("Requesting a new Nexus OAuth access token from {OAuthEndpoint}.", settings.OAuthEndpoint);

        var response = await pipeline.ExecuteAsync(async ct =>
        {
            using var tokenRequest = new HttpRequestMessage(HttpMethod.Post, settings.OAuthEndpoint)
            {
                Content = new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    ["grant_type"] = "client_credentials",
                    ["client_id"] = settings.ClientId,
                    ["client_secret"] = settings.ClientSecret,
                    ["scope"] = settings.Scope,
                }),
            };

            return await httpClient.SendAsync(tokenRequest, ct);
        }, cancellationToken);

        response.EnsureSuccessStatusCode();

        var payload = await response.Content.ReadFromJsonAsync<TokenResponse>(cancellationToken: cancellationToken)
            ?? throw new InvalidOperationException("Nexus OAuth token endpoint returned an empty response.");

        logger.LogDebug("Acquired a new Nexus OAuth access token, expiring in {ExpiresInSeconds}s.", payload.ExpiresIn);

        return new CachedToken(payload.AccessToken, DateTimeOffset.UtcNow.AddSeconds(payload.ExpiresIn));
    }

    private sealed record CachedToken(string AccessToken, DateTimeOffset ExpiresAtUtc);

    private sealed record TokenResponse(
        [property: JsonPropertyName("access_token")] string AccessToken,
        [property: JsonPropertyName("expires_in")] int ExpiresIn);
}
