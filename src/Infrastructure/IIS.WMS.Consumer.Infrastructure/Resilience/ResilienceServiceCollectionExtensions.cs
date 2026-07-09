using Azure.Messaging.ServiceBus;
using Azure;
using Microsoft.Extensions.DependencyInjection;
using Polly;

namespace IIS.WMS.Consumer.Infrastructure.Resilience;

/// <summary>
/// Polly v8 pipelines for transient infrastructure faults on Service Bus publish, Blob Storage,
/// and outbound HTTP calls (integration-resiliency.instructions.md §3). Deliberately does not
/// cover Cosmos 429s (the Cosmos SDK's own retry options, cosmos-db.instructions.md §2) or ETag
/// 412 conflicts (the re-read-and-reapply loop, integration-resiliency.instructions.md §2) - those
/// need different recovery strategies than a blind retry of the same delegate.
/// </summary>
public static class ResilienceServiceCollectionExtensions
{
    /// <summary>Registers the three named Polly pipelines in <see cref="ResiliencePipelines"/>, resolved by key via <c>ResiliencePipelineProvider&lt;string&gt;</c>.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddResiliencePipelines(this IServiceCollection services)
    {
        services.AddResiliencePipeline(ResiliencePipelines.ServiceBusPublish, builder => builder
            .AddRetry(new Polly.Retry.RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<ServiceBusException>(ex => ex.IsTransient),
                MaxRetryAttempts = 5,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = TimeSpan.FromMilliseconds(200),
            })
            .AddCircuitBreaker(new Polly.CircuitBreaker.CircuitBreakerStrategyOptions
            {
                FailureRatio = 0.5,
                SamplingDuration = TimeSpan.FromSeconds(30),
                MinimumThroughput = 10,
                BreakDuration = TimeSpan.FromSeconds(15),
            }));

        services.AddResiliencePipeline(ResiliencePipelines.BlobUpload, builder => builder
            .AddRetry(new Polly.Retry.RetryStrategyOptions
            {
                // Azure.Storage.Blobs throws RequestFailedException; IsTransient isn't exposed on
                // it the way ServiceBusException exposes it, so match on retryable status codes.
                ShouldHandle = new PredicateBuilder().Handle<RequestFailedException>(
                    ex => ex.Status is 408 or 429 or >= 500),
                MaxRetryAttempts = 5,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = TimeSpan.FromMilliseconds(200),
            }));

        // Polly.Extensions' AddResiliencePipeline<TKey, TResult> takes both type parameters -
        // explicit generic args in C# are all-or-nothing, so both must be given even though TKey
        // is always string here (integration-resiliency.instructions.md §3 shows a single-arg
        // call, which doesn't compile against this package's actual two-type-parameter overload).
        services.AddResiliencePipeline<string, HttpResponseMessage>(ResiliencePipelines.OutboundHttp, builder => builder
            .AddRetry(new Polly.Retry.RetryStrategyOptions<HttpResponseMessage>
            {
                ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                    .Handle<HttpRequestException>()
                    .HandleResult(r => (int)r.StatusCode is 408 or 429 or >= 500),
                MaxRetryAttempts = 5,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = TimeSpan.FromMilliseconds(200),
            }));

        return services;
    }
}
