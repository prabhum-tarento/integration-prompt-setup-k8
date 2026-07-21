using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="ConsumerHealthCheck"/> - unhealthy once the paired event type's
/// last successful poll exceeds the (fixed, 5 minute) staleness window; an idle topic with a recent
/// poll is still healthy (integration-resiliency.instructions.md §8).
/// </summary>
public class ConsumerHealthCheckTests
{
    [Fact(DisplayName = "CheckHealthAsync reports Healthy when the last poll is well within the staleness window")]
    public async Task CheckHealthAsync_RecentPoll_ReportsHealthy()
    {
        var state = new ConsumerHealthState { LastSuccessfulPollUtc = DateTimeOffset.UtcNow };
        var sut = new ConsumerHealthCheck(state, "InventoryStateChanged", Substitute.For<ILogger<ConsumerHealthCheck>>());

        var result = await sut.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact(DisplayName = "CheckHealthAsync reports Healthy just under the staleness window boundary")]
    public async Task CheckHealthAsync_JustUnderStalenessWindow_ReportsHealthy()
    {
        var state = new ConsumerHealthState
        {
            LastSuccessfulPollUtc = DateTimeOffset.UtcNow - TimeSpan.FromMinutes(5) + TimeSpan.FromSeconds(5),
        };
        var sut = new ConsumerHealthCheck(state, "InventoryStateChanged", Substitute.For<ILogger<ConsumerHealthCheck>>());

        var result = await sut.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact(DisplayName = "CheckHealthAsync reports Unhealthy just over the staleness window boundary")]
    public async Task CheckHealthAsync_JustOverStalenessWindow_ReportsUnhealthy()
    {
        var state = new ConsumerHealthState
        {
            LastSuccessfulPollUtc = DateTimeOffset.UtcNow - TimeSpan.FromMinutes(5) - TimeSpan.FromSeconds(5),
        };
        var sut = new ConsumerHealthCheck(state, "InventoryStateChanged", Substitute.For<ILogger<ConsumerHealthCheck>>());

        var result = await sut.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact(DisplayName = "CheckHealthAsync reports Unhealthy when the last poll is long past the staleness window")]
    public async Task CheckHealthAsync_StalePoll_ReportsUnhealthy()
    {
        var state = new ConsumerHealthState { LastSuccessfulPollUtc = DateTimeOffset.UtcNow - TimeSpan.FromHours(1) };
        var sut = new ConsumerHealthCheck(state, "InventoryAdjusted", Substitute.For<ILogger<ConsumerHealthCheck>>());

        var result = await sut.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("No successful poll", result.Description);
    }
}
