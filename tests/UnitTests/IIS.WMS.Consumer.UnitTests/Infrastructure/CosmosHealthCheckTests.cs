using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>Correctness tests for <see cref="CosmosHealthCheck"/> - the Api readiness probe for Cosmos DB connectivity.</summary>
public class CosmosHealthCheckTests
{
    private const string DatabaseName = "InventoryDb";

    private static IOptions<CosmosDbOptions> BuildOptions() =>
        Options.Create(new CosmosDbOptions { DatabaseName = DatabaseName });

    [Fact(DisplayName = "CheckHealthAsync returns Healthy when the database metadata read succeeds")]
    public async Task CheckHealthAsync_DatabaseReadSucceeds_ReturnsHealthy()
    {
        var client = Substitute.For<CosmosClient>();
        var database = Substitute.For<Database>();
        client.GetDatabase(DatabaseName).Returns(database);
        database.ReadAsync(requestOptions: Arg.Any<RequestOptions>(), cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<DatabaseResponse>(null!));
        var check = new CosmosHealthCheck(client, BuildOptions(), Substitute.For<ILogger<CosmosHealthCheck>>());

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.Null(result.Exception);
    }

    [Fact(DisplayName = "CheckHealthAsync returns Unhealthy with the exception attached when the database read throws")]
    public async Task CheckHealthAsync_DatabaseReadThrows_ReturnsUnhealthyWithException()
    {
        var client = Substitute.For<CosmosClient>();
        var database = Substitute.For<Database>();
        var thrown = new InvalidOperationException("Cosmos is unreachable.");
        client.GetDatabase(DatabaseName).Returns(database);
        database.ReadAsync(requestOptions: Arg.Any<RequestOptions>(), cancellationToken: Arg.Any<CancellationToken>())
            .Throws(thrown);
        var logger = Substitute.For<ILogger<CosmosHealthCheck>>();
        var check = new CosmosHealthCheck(client, BuildOptions(), logger);

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Same(thrown, result.Exception);
        Assert.Equal("Could not reach the Cosmos DB database.", result.Description);
    }
}
