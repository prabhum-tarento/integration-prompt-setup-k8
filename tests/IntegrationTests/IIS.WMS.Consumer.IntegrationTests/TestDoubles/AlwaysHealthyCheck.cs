using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles;

/// <summary>Replaces the Cosmos/Kafka/Service Bus health checks in <see cref="CustomWebApplicationFactory"/> - those real checks need live dependencies this test host doesn't have.</summary>
public sealed class AlwaysHealthyCheck : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default) =>
        Task.FromResult(HealthCheckResult.Healthy());
}
