using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;

/// <summary>
/// Registers the background MessageArchive pipeline: the bounded <see cref="Channel{T}"/>,
/// <see cref="IMessageArchiveWriter"/>, <see cref="MessageArchiveBackgroundService"/>, and whichever
/// <see cref="IMessageArchiveSink"/> destination(s) <see cref="MessageArchiveOptions.CosmosDbEnabled"/>/
/// <see cref="MessageArchiveOptions.BlobEnabled"/> select - mirroring
/// <c>Persistence.CosmosDb.Audit.AuditServiceCollectionExtensions.AddAuditTrail</c>, with one deliberate
/// divergence: unlike that method, this one does <b>not</b> throw when both toggles are
/// <see langword="false"/> - see <see cref="MessageArchiveOptions.CosmosDbEnabled"/>'s remarks for why a
/// message archive with nowhere to persist to is tolerated (zero <see cref="IMessageArchiveSink"/>
/// registrations, and <see cref="MessageArchiveBackgroundService"/>'s <c>Task.WhenAll</c> fan-out is
/// naturally a no-op over an empty sink list).
/// </summary>
public static class MessageArchiveServiceCollectionExtensions
{
    /// <summary>Registers the MessageArchive channel, writer, background worker, and the configured <see cref="IMessageArchiveSink"/> destination(s), if any.</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>MessageArchive</c> section.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    public static IServiceCollection AddMessageArchiving(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<MessageArchiveOptions>(configuration.GetSection(MessageArchiveOptions.SectionName));

        // Bounded, not unbounded: a sustained Cosmos/Blob outage must not grow this channel's backing
        // buffer without limit (integration-resiliency.instructions.md §6). Wait mode paired with
        // MessageArchiveWriter.Enqueue's use of TryWrite (never WriteAsync) is what makes a full channel
        // return false immediately instead of blocking the caller - see that class's remarks.
        services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<MessageArchiveOptions>>().Value;

            return Channel.CreateBounded<MessageArchive>(new BoundedChannelOptions(options.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
            });
        });

        services.AddSingleton<IMessageArchiveWriter, MessageArchiveWriter>();
        services.AddHostedService<MessageArchiveBackgroundService>();

        // Read synchronously here (same configuration.GetSection(...).Get<T>() pattern
        // AddAuditTrail/CosmosDbServiceCollectionExtensions use) to decide which IMessageArchiveSink(s)
        // to register - this registration-time decision, not a runtime branch inside
        // MessageArchiveBackgroundService, is what lets that class stay ignorant of Cosmos/Blob Storage
        // entirely. Unlike AddAuditTrail, deliberately no throw when both toggles are false - see this
        // class's remarks.
        var messageArchiveOptions = configuration.GetSection(MessageArchiveOptions.SectionName).Get<MessageArchiveOptions>() ?? new MessageArchiveOptions();

        if (messageArchiveOptions.CosmosDbEnabled)
        {
            services.AddScoped<IMessageArchiveSink, CosmosMessageArchiveSink>();
        }

        if (messageArchiveOptions.BlobEnabled)
        {
            services.AddScoped<IMessageArchiveSink, BlobMessageArchiveSink>();
        }

        return services;
    }
}
