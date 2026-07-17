using System.Net;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.NexusServices;
using IIS.WMS.Common.Resilience;
using IIS.WMS.Consumer.UnitTests.Infrastructure.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Polly;
using Polly.Registry;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="NexusDeduplicationService"/> - the composite dedup-id
/// construction, the "no header" short-circuit that skips calling Nexus entirely, and the
/// 409-means-duplicate response contract (integration-resiliency.instructions.md §1).
/// </summary>
public class NexusDeduplicationServiceTests
{
    private const string ConsumerName = "InventoryEvents Kafka consumer";
    private const string CorrelationId = "corr-1";

    [Fact(DisplayName = "A missing Deduplication-Id header skips the check without calling Nexus")]
    public async Task IsDuplicateAsync_MissingDeduplicationId_ReturnsFalseWithoutCallingNexus()
    {
        var stubHandler = new StubHttpMessageHandler();
        var service = CreateService(stubHandler);

        var result = await service.IsDuplicateAsync(ConsumerName, deduplicationId: string.Empty, CorrelationId);

        Assert.False(result);
        Assert.Empty(stubHandler.Requests);
    }

    [Fact(DisplayName = "A 409 Conflict from Nexus is reported as a duplicate")]
    public async Task IsDuplicateAsync_NexusReturnsConflict_ReturnsTrue()
    {
        var stubHandler = new StubHttpMessageHandler
        {
            Respond = _ => new HttpResponseMessage(HttpStatusCode.Conflict),
        };
        var service = CreateService(stubHandler);

        var result = await service.IsDuplicateAsync(ConsumerName, "dedup-1", CorrelationId);

        Assert.True(result);
        Assert.Single(stubHandler.Requests);
    }

    [Fact(DisplayName = "A success response from Nexus is reported as not a duplicate")]
    public async Task IsDuplicateAsync_NexusReturnsSuccess_ReturnsFalse()
    {
        var stubHandler = new StubHttpMessageHandler
        {
            Respond = _ => new HttpResponseMessage(HttpStatusCode.OK),
        };
        var service = CreateService(stubHandler);

        var result = await service.IsDuplicateAsync(ConsumerName, "dedup-1", CorrelationId);

        Assert.False(result);
    }

    [Fact(DisplayName = "A non-success, non-409 response from Nexus throws rather than being swallowed")]
    public async Task IsDuplicateAsync_NexusReturnsServerError_Throws()
    {
        var stubHandler = new StubHttpMessageHandler
        {
            Respond = _ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new StringContent("boom"),
            },
        };
        var service = CreateService(stubHandler);

        await Assert.ThrowsAsync<HttpRequestException>(
            () => service.IsDuplicateAsync(ConsumerName, "dedup-1", CorrelationId));
    }

    [Fact(DisplayName = "The Correlation-Id header sent to Nexus carries the message's correlation id, not the dedup id")]
    public async Task IsDuplicateAsync_SendsCorrelationIdHeader()
    {
        var stubHandler = new StubHttpMessageHandler();
        var service = CreateService(stubHandler);

        await service.IsDuplicateAsync(ConsumerName, "dedup-1", CorrelationId);

        var sentRequest = Assert.Single(stubHandler.Requests);
        Assert.Equal(CorrelationId, sentRequest.Headers.GetValues("Correlation-Id").Single());
    }

    private static NexusDeduplicationService CreateService(StubHttpMessageHandler stubHandler)
    {
        var httpClient = new HttpClient(stubHandler) { BaseAddress = new Uri("https://nexus.example/dedup") };

        var services = new ServiceCollection();
        // An empty (no retry, no circuit breaker) pipeline - these tests exercise
        // NexusDeduplicationService's own logic, not Polly's retry behavior.
        services.AddResiliencePipeline<string, HttpResponseMessage>(ResiliencePipelines.OutboundHttp, _ => { });
        var pipelineProvider = services.BuildServiceProvider().GetRequiredService<ResiliencePipelineProvider<string>>();

        var applicationOptions = Options.Create(new ApplicationOptions
        {
            AppName = "IIS.WMS.Consumer",
            AppId = "iis-wms-consumer",
        });

        return new NexusDeduplicationService(httpClient, pipelineProvider, applicationOptions, Substitute.For<ILogger<NexusDeduplicationService>>());
    }
}
