using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;

/// <summary>Verifies the management client can reach the namespace (integration-resiliency.instructions.md §8) - not a staleness check like the Kafka consumer's, since an idle queue is a normal, healthy state.</summary>
public sealed class ServiceBusHealthCheck(
    ServiceBusAdministrationClient administrationClient,
    IOptions<ServiceBusConsumerOptions> options,
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
            await administrationClient.GetQueueRuntimePropertiesAsync(options.Value.QueueName, cancellationToken);

            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Service Bus health check failed - could not reach the namespace.");

            return HealthCheckResult.Unhealthy("Could not reach the Service Bus namespace.", ex);
        }
    }
}
