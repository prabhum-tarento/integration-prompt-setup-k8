using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;

/// <summary>
/// Verifies the management client can reach a named queue (integration-resiliency.instructions.md
/// §8) - not a staleness check like the Kafka consumer's, since an idle queue is a normal, healthy
/// state. Takes <paramref name="queueName"/> directly rather than <c>IOptions&lt;ServiceBusConsumerOptions&gt;</c>
/// so the same check type can be registered once per queue (the session-enabled
/// <c>inventory-events</c> queue and the non-session bulk-import queue) via
/// <c>AddTypeActivatedCheck</c>, instead of duplicating this class per queue.
/// </summary>
public sealed class ServiceBusHealthCheck(
    ServiceBusAdministrationClient administrationClient,
    string queueName,
    ILogger<ServiceBusHealthCheck> logger) : IHealthCheck
{
    /// <summary>Attempts to read the target queue's runtime properties; healthy if it succeeds, unhealthy (with the exception attached) otherwise.</summary>
    /// <param name="context">Health check execution context - unused, this check has no configurable failure status.</param>
    /// <param name="cancellationToken">Token to cancel the check.</param>
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await administrationClient.GetQueueRuntimePropertiesAsync(queueName, cancellationToken);

            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Service Bus health check failed for queue {QueueName} - could not reach the namespace.", queueName);

            return HealthCheckResult.Unhealthy($"Could not reach queue '{queueName}' on the Service Bus namespace.", ex);
        }
    }
}
