using System.Threading.Channels;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;

/// <summary>
/// Registers the background audit-trail pipeline every <c>CosmosRepository{TDomain,TDocument}</c>
/// mutation feeds into: the bounded <see cref="Channel{T}"/>, <see cref="IAuditTrailWriter"/>,
/// <see cref="AuditBackgroundService"/>, and whichever <see cref="IAuditSink"/> destination(s)
/// <see cref="AuditOptions.CosmosDbEnabled"/>/<see cref="AuditOptions.ColdStorageEnabled"/> select.
/// Called from <c>CosmosDbServiceCollectionExtensions.AddCosmosDb</c>, since the audit pipeline only
/// exists to serve Cosmos repositories.
/// </summary>
public static class AuditServiceCollectionExtensions
{
    /// <summary>Registers the audit channel, writer, background worker, and the configured <see cref="IAuditSink"/> destination(s).</summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configuration">Application configuration, read for the <c>Audit</c> section.</param>
    /// <returns>The same <paramref name="services"/> instance, for chaining.</returns>
    /// <exception cref="InvalidOperationException">Neither <see cref="AuditOptions.CosmosDbEnabled"/> nor <see cref="AuditOptions.ColdStorageEnabled"/> is <see langword="true"/> - the pipeline would drain into no destination at all.</exception>
    public static IServiceCollection AddAuditTrail(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<AuditOptions>(configuration.GetSection(AuditOptions.SectionName));

        // Bounded, not unbounded: a sustained Cosmos/Audit-container outage must not grow this
        // channel's backing buffer without limit (integration-resiliency.instructions.md §6). Wait
        // mode paired with AuditTrailWriter.Enqueue's use of TryWrite (never WriteAsync) is what makes
        // a full channel return false immediately instead of blocking the caller - see that class's
        // remarks.
        services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<AuditOptions>>().Value;

            return Channel.CreateBounded<AuditEntry>(new BoundedChannelOptions(options.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
            });
        });

        services.AddSingleton<IAuditTrailWriter, AuditTrailWriter>();
        services.AddHostedService<AuditBackgroundService>();

        // Read synchronously here (same configuration.GetSection(...).Get<T>() pattern
        // CosmosDbServiceCollectionExtensions uses) to decide which IAuditSink(s) to register - this
        // registration-time decision, not a runtime branch inside AuditBackgroundService, is what lets
        // that class stay ignorant of Cosmos/Blob Storage entirely.
        var auditOptions = configuration.GetSection(AuditOptions.SectionName).Get<AuditOptions>() ?? new AuditOptions();

        if (!auditOptions.CosmosDbEnabled && !auditOptions.ColdStorageEnabled)
        {
            throw new InvalidOperationException(
                $"At least one of '{AuditOptions.SectionName}:{nameof(AuditOptions.CosmosDbEnabled)}' or " +
                $"'{AuditOptions.SectionName}:{nameof(AuditOptions.ColdStorageEnabled)}' must be true - " +
                "otherwise every drained audit entry has nowhere to persist to.");
        }

        if (auditOptions.CosmosDbEnabled)
        {
            services.AddScoped<IAuditSink, CosmosAuditSink>();

            // Explicit factory, not AddScoped<IAuditRepository, AuditRepository>() - AuditRepository
            // must never receive the real IAuditTrailWriter singleton, or persisting an audit record
            // would itself enqueue another audit record forever. See AuditRepository's own remarks.
            services.AddScoped<IAuditRepository>(sp => new AuditRepository(
                sp.GetRequiredService<ICosmosContainerFactory>(),
                sp.GetRequiredService<ILogger<AuditRepository>>(),
                sp.GetRequiredService<ICorrelationContext>(),
                NullAuditTrailWriter.Instance));
        }

        if (auditOptions.ColdStorageEnabled)
        {
            services.AddScoped<IAuditSink, ColdBlobAuditSink>();
        }

        return services;
    }
}
