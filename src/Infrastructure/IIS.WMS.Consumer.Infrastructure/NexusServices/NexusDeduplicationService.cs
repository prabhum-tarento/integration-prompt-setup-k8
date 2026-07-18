using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Resilience;
using IIS.WMS.Consumer.Application.Common;
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
    IOptions<ApplicationOptions> applicationOptions,
    ILogger<NexusDeduplicationService> logger)
    : IDeduplicationService
{
    /// <inheritdoc />
    public async Task<bool> IsDuplicateAsync(
        string consumerName, string deduplicationId, string correlationId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(deduplicationId))
        {
            logger.LogWarning(
                "{ConsumerName}: message for correlation {CorrelationId} had no {DeduplicationIdHeader} header - skipping deduplication check for it.",
                consumerName, correlationId, WellKnownHeaderNames.DeduplicationId);
            return false;
        }

        var compositeDeduplicationId = $"{applicationOptions.Value.AppId}_{consumerName}_{deduplicationId}_{correlationId}";
        var stopwatch = Stopwatch.StartNew();

        var pipeline = pipelineProvider.GetPipeline<HttpResponseMessage>(ResiliencePipelines.OutboundHttp);

        var response = await pipeline.ExecuteAsync(async ct =>
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, string.Empty)
            {
                Content = JsonContent.Create(new DeduplicationRequest(compositeDeduplicationId)),
            };
            request.Headers.Add(WellKnownHeaderNames.CorrelationId, correlationId);

            return await httpClient.SendAsync(request, ct);
        }, cancellationToken);

        if (response.StatusCode == HttpStatusCode.Conflict)
        {
            logger.LogInformation(
                "Nexus reported {DeduplicationId} (correlation {CorrelationId}) as a duplicate.",
                compositeDeduplicationId, correlationId);
            LogCompleted(compositeDeduplicationId, correlationId, stopwatch.Elapsed, isDuplicate: true);
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

        LogCompleted(compositeDeduplicationId, correlationId, stopwatch.Elapsed, isDuplicate: false);
        return false;
    }

    /// <summary>
    /// Debug-level completion log for one <see cref="IsDuplicateAsync"/> call - owned here rather than
    /// by <c>KafkaConsumerHostedServiceBase</c>, which only needs the elapsed duration itself (folded into its
    /// own per-message "relayed" summary log line), not a duplicate log line per call.
    /// </summary>
    private void LogCompleted(string compositeDeduplicationId, string correlationId, TimeSpan elapsed, bool isDuplicate) =>
        logger.LogDebug(
            "Nexus deduplication check completed for {DeduplicationId} (correlation {CorrelationId}) in {DeduplicationDurationMs}ms - IsDuplicate: {IsDuplicate}.",
            compositeDeduplicationId, correlationId, elapsed.TotalMilliseconds, isDuplicate);

    private sealed record DeduplicationRequest([property: JsonPropertyName("dedupeId")] string DeduplicationId);
}
