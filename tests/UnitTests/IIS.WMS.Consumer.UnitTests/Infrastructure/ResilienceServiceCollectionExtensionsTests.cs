using Azure;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Common.Resilience;
using IIS.WMS.Consumer.Infrastructure.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Polly.Registry;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Registration and predicate-matching tests for <see cref="ResilienceServiceCollectionExtensions.AddResiliencePipelines"/> -
/// the three named Polly v8 pipelines resolved via <see cref="ResiliencePipelineProvider{TKey}"/>
/// (integration-resiliency.instructions.md §3).
/// </summary>
public class ResilienceServiceCollectionExtensionsTests
{
    private static ResiliencePipelineProvider<string> BuildProvider()
    {
        var services = new ServiceCollection();
        services.AddResiliencePipelines();

        return services.BuildServiceProvider().GetRequiredService<ResiliencePipelineProvider<string>>();
    }

    [Fact(DisplayName = "AddResiliencePipelines returns the same collection for chaining and registers all three named pipelines")]
    public void AddResiliencePipelines_Registered_AllThreeKeysResolve()
    {
        var services = new ServiceCollection();

        var result = services.AddResiliencePipelines();

        Assert.Same(services, result);

        var pipelineProvider = services.BuildServiceProvider().GetRequiredService<ResiliencePipelineProvider<string>>();

        Assert.NotNull(pipelineProvider.GetPipeline(ResiliencePipelines.ServiceBusPublish));
        Assert.NotNull(pipelineProvider.GetPipeline(ResiliencePipelines.BlobUpload));
        Assert.NotNull(pipelineProvider.GetPipeline<HttpResponseMessage>(ResiliencePipelines.OutboundHttp));
    }

    [Fact(DisplayName = "service-bus-publish does not retry a non-transient ServiceBusException")]
    public async Task ServiceBusPublish_NonTransientException_PropagatesWithoutRetry()
    {
        var pipeline = BuildProvider().GetPipeline(ResiliencePipelines.ServiceBusPublish);
        var attempts = 0;

        await Assert.ThrowsAsync<ServiceBusException>(() => pipeline.ExecuteAsync(async _ =>
        {
            attempts++;
            await Task.Yield();
            throw new ServiceBusException(isTransient: false, message: "non-transient");
        }).AsTask());

        Assert.Equal(1, attempts);
    }

    [Fact(DisplayName = "service-bus-publish does not retry an exception type the predicate doesn't handle")]
    public async Task ServiceBusPublish_UnhandledExceptionType_PropagatesWithoutRetry()
    {
        var pipeline = BuildProvider().GetPipeline(ResiliencePipelines.ServiceBusPublish);
        var attempts = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.ExecuteAsync(async _ =>
        {
            attempts++;
            await Task.Yield();
            throw new InvalidOperationException("not a service bus fault");
        }).AsTask());

        Assert.Equal(1, attempts);
    }

    [Fact(DisplayName = "service-bus-publish retries a transient ServiceBusException")]
    public async Task ServiceBusPublish_TransientException_Retries()
    {
        var pipeline = BuildProvider().GetPipeline(ResiliencePipelines.ServiceBusPublish);
        var attempts = 0;

        await pipeline.ExecuteAsync(async _ =>
        {
            attempts++;
            await Task.Yield();
            if (attempts == 1)
            {
                throw new ServiceBusException(isTransient: true, message: "transient");
            }
        });

        Assert.Equal(2, attempts);
    }

    [Fact(DisplayName = "blob-upload does not retry a non-retryable status code")]
    public async Task BlobUpload_NonRetryableStatus_PropagatesWithoutRetry()
    {
        var pipeline = BuildProvider().GetPipeline(ResiliencePipelines.BlobUpload);
        var attempts = 0;

        await Assert.ThrowsAsync<RequestFailedException>(() => pipeline.ExecuteAsync(async _ =>
        {
            attempts++;
            await Task.Yield();
            throw new RequestFailedException(404, "not found");
        }).AsTask());

        Assert.Equal(1, attempts);
    }

    [Theory(DisplayName = "blob-upload retries a retryable RequestFailedException status")]
    [InlineData(408)]
    [InlineData(429)]
    [InlineData(500)]
    [InlineData(503)]
    public async Task BlobUpload_RetryableStatus_Retries(int status)
    {
        var pipeline = BuildProvider().GetPipeline(ResiliencePipelines.BlobUpload);
        var attempts = 0;

        await pipeline.ExecuteAsync(async _ =>
        {
            attempts++;
            await Task.Yield();
            if (attempts == 1)
            {
                throw new RequestFailedException(status, "transient");
            }
        });

        Assert.Equal(2, attempts);
    }

    [Fact(DisplayName = "outbound-http does not retry a successful (non-retryable) response")]
    public async Task OutboundHttp_SuccessResult_NoRetry()
    {
        var pipeline = BuildProvider().GetPipeline<HttpResponseMessage>(ResiliencePipelines.OutboundHttp);
        var attempts = 0;

        var response = await pipeline.ExecuteAsync(async _ =>
        {
            attempts++;
            await Task.Yield();
            return new HttpResponseMessage(System.Net.HttpStatusCode.OK);
        });

        Assert.Equal(1, attempts);
        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
    }

    [Fact(DisplayName = "outbound-http retries on a retryable result status code")]
    public async Task OutboundHttp_RetryableResultStatus_Retries()
    {
        var pipeline = BuildProvider().GetPipeline<HttpResponseMessage>(ResiliencePipelines.OutboundHttp);
        var attempts = 0;

        var response = await pipeline.ExecuteAsync(async _ =>
        {
            attempts++;
            await Task.Yield();
            return new HttpResponseMessage(attempts == 1 ? System.Net.HttpStatusCode.ServiceUnavailable : System.Net.HttpStatusCode.OK);
        });

        Assert.Equal(2, attempts);
        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
    }

    [Fact(DisplayName = "outbound-http retries on HttpRequestException regardless of status")]
    public async Task OutboundHttp_HttpRequestException_Retries()
    {
        var pipeline = BuildProvider().GetPipeline<HttpResponseMessage>(ResiliencePipelines.OutboundHttp);
        var attempts = 0;

        var response = await pipeline.ExecuteAsync(async _ =>
        {
            attempts++;
            await Task.Yield();
            if (attempts == 1)
            {
                throw new HttpRequestException("network blip");
            }

            return new HttpResponseMessage(System.Net.HttpStatusCode.OK);
        });

        Assert.Equal(2, attempts);
        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
    }
}
