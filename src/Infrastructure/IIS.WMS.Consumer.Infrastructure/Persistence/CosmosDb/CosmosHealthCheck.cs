using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>
/// Api readiness check for Cosmos DB connectivity (aspnet-rest-apis.instructions.md "Health checks"). Reads
/// the database itself rather than any one container - now that each repository declares its own container
/// name (cosmos-db.instructions.md §1), there is no single container this check could point at that would
/// prove connectivity on behalf of every repository, so a database metadata read is the shared reachability
/// signal instead.
/// </summary>
public sealed class CosmosHealthCheck(CosmosClient client, IOptions<CosmosDbOptions> options, ILogger<CosmosHealthCheck> logger)
    : IHealthCheck
{
    /// <summary>Attempts a database metadata read; healthy if it succeeds, unhealthy (with the exception attached) otherwise.</summary>
    /// <param name="context">Health check execution context - unused, this check has no configurable failure status.</param>
    /// <param name="cancellationToken">Token to cancel the check.</param>
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await client.GetDatabase(options.Value.DatabaseName).ReadAsync(cancellationToken: cancellationToken);

            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Cosmos DB health check failed - could not reach the database.");

            return HealthCheckResult.Unhealthy("Could not reach the Cosmos DB database.", ex);
        }
    }
}
