using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.OrderArchiving;

/// <summary>
/// Registers the background OrderArchive pipeline: the bounded <see cref="Channel{T}"/>,
/// <see cref="IOrderArchiveWriter"/>, and <see cref="OrderArchiveBackgroundService"/> - decouples
/// <c>ConsumerHostedService</c>'s per-message Kafka-worker latency from the Cosmos round-trip an
/// <see cref="OrderArchive"/> upsert costs (integration-resiliency.instructions.md §6), mirroring
/// <c>AuditServiceCollectionExtensions.AddAuditTrail</c>.
/// </summary>
public static class OrderArchiveServiceCollectionExtensions
{
    /// <summary>Registers the OrderArchive channel, writer, and background worker.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>OrderArchive</c> section.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddOrderArchiving(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<OrderArchiveOptions>(configuration.GetSection(OrderArchiveOptions.SectionName));

        // Bounded, not unbounded: a sustained Cosmos/OrderArchive-container outage must not grow this
        // channel's backing buffer without limit (integration-resiliency.instructions.md §6). Wait
        // mode paired with OrderArchiveWriter.Enqueue's use of TryWrite (never WriteAsync) is what makes
        // a full channel return false immediately instead of blocking the caller - see that class's
        // remarks.
        services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<OrderArchiveOptions>>().Value;

            return Channel.CreateBounded<OrderArchive>(new BoundedChannelOptions(options.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
            });
        });

        services.AddSingleton<IOrderArchiveWriter, OrderArchiveWriter>();
        services.AddHostedService<OrderArchiveBackgroundService>();

        return services;
    }
}
