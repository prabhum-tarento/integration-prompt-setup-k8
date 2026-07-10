using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using IIS.WMS.Consumer.Infrastructure.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly.Registry;

namespace IIS.WMS.Consumer.Infrastructure.NexusServices;

/// <inheritdoc cref="IDeduplicationService" />
/// <remarks>
/// Calls the external Nexus deduplication API with a single <c>POST</c> carrying a composite
/// deduplication id in the body. Nexus signals "already seen" with <c>409 Conflict</c> rather than a
/// body flag - any other non-success status is a genuine failure and is thrown, not swallowed, so the
/// caller (the Kafka consumer's dedup check) treats a Nexus outage as a processing failure for that
/// message rather than silently treating it as "not a duplicate."
/// </remarks>
public sealed class NexusDeduplicationService(
    HttpClient httpClient,
    ResiliencePipelineProvider<string> pipelineProvider,
    IOptions<NexusDeduplicationOptions> options,
    ILogger<NexusDeduplicationService> logger)
    : IDeduplicationService
{
    /// <inheritdoc />
    public async Task<bool> IsDuplicateAsync(
        string consumerName, string deduplicationId, string correlationId, CancellationToken cancellationToken = default)
    {
        var settings = options.Value;

        if (!settings.Enabled)
        {
            logger.LogInformation("Deduplication check is disabled via configuration - skipping for correlation {CorrelationId}.", correlationId);
            return false;
        }

        if (string.IsNullOrEmpty(deduplicationId))
        {
            logger.LogWarning(
                "{ConsumerName}: message for correlation {CorrelationId} had no {DeduplicationIdHeader} header - skipping deduplication check for it.",
                consumerName, correlationId, KafkaHeaderNames.DeduplicationId);
            return false;
        }

        var compositeDeduplicationId = $"{settings.AppId}_{consumerName}_{deduplicationId}_{correlationId}";

        logger.LogDebug(
            "Checking Nexus deduplication for {DeduplicationId}, correlation {CorrelationId}.",
            compositeDeduplicationId, correlationId);

        var pipeline = pipelineProvider.GetPipeline<HttpResponseMessage>(ResiliencePipelines.OutboundHttp);

        var response = await pipeline.ExecuteAsync(async ct =>
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, string.Empty)
            {
                Content = JsonContent.Create(new DeduplicationRequest(compositeDeduplicationId)),
            };
            request.Headers.Add(KafkaHeaderNames.CorrelationId, correlationId);

            return await httpClient.SendAsync(request, ct);
        }, cancellationToken);

        if (response.StatusCode == HttpStatusCode.Conflict)
        {
            logger.LogInformation(
                "Nexus reported {DeduplicationId} (correlation {CorrelationId}) as a duplicate.",
                compositeDeduplicationId, correlationId);
            return true;
        }

        if (!response.IsSuccessStatusCode)
        {
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException(
                $"Nexus deduplication check failed with {(int)response.StatusCode} {response.StatusCode} " +
                $"for {compositeDeduplicationId} (correlation {correlationId}). Response body: {responseBody}",
                inner: null,
                response.StatusCode);
        }

        return false;
    }

    private sealed record DeduplicationRequest([property: JsonPropertyName("dedupeId")] string DeduplicationId);
}
