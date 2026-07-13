using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Unhealthy if the paired consumer event type's last successful poll exceeds a staleness window - an
/// idle topic isn't a failure, so this does not require a message to have been processed
/// (integration-resiliency.instructions.md §8). One instance is registered per event type a consumer
/// registers via <see cref="ConsumerHostedService.RegisterSchemaHandlers"/> (see
/// <c>MessagingServiceCollectionExtensions.AddKafkaConsumer</c>, which resolves each event type's own
/// <see cref="ConsumerHealthState"/> off the consumer via <see cref="ConsumerHostedService.GetHealthState"/>
/// at check time), each given its own display name for the log messages below.
/// </summary>
public sealed class ConsumerHealthCheck(ConsumerHealthState state, string consumerName, ILogger<ConsumerHealthCheck> logger)
    : IHealthCheck
{
    private static readonly TimeSpan StalenessWindow = TimeSpan.FromMinutes(5);

    /// <summary>Compares the time since the last successful poll against <see cref="StalenessWindow"/>.</summary>
    /// <param name="context">Health check execution context - unused, this check has no configurable failure status.</param>
    /// <param name="cancellationToken">Token to cancel the check (unused - this check is purely in-memory).</param>
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var staleness = DateTimeOffset.UtcNow - state.LastSuccessfulPollUtc;

        if (staleness > StalenessWindow)
        {
            logger.LogWarning(
                "{ConsumerName} health check unhealthy: no successful poll in {StalenessSeconds:F0}s (window {WindowSeconds:F0}s).",
                consumerName, staleness.TotalSeconds, StalenessWindow.TotalSeconds);

            return Task.FromResult(HealthCheckResult.Unhealthy(
                $"No successful poll in {staleness.TotalSeconds:F0}s (staleness window {StalenessWindow.TotalSeconds:F0}s)."));
        }

        logger.LogDebug("{ConsumerName} health check healthy: last poll {StalenessSeconds:F0}s ago.", consumerName, staleness.TotalSeconds);

        return Task.FromResult(HealthCheckResult.Healthy($"Last poll {staleness.TotalSeconds:F0}s ago."));
    }
}
