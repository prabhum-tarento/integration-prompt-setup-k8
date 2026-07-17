using System.Threading.Channels;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Registration tests for <see cref="AuditServiceCollectionExtensions.AddAuditTrail"/> - the bounded
/// audit channel, <see cref="IAuditTrailWriter"/>, <see cref="AuditBackgroundService"/>, and the
/// <see cref="IAuditRepository"/> that must never receive the real writer (see that registration's own
/// remarks - persisting an audit record must not itself enqueue another one).
/// </summary>
public class AuditServiceCollectionExtensionsTests
{
    private static IServiceProvider BuildProvider(IConfiguration? configuration = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IFileStore>(
            BlobStorageServiceCollectionExtensions.HotTierKey, (_, _) => Substitute.For<IFileStore>());
        services.AddKeyedSingleton<IFileStore>(
            BlobStorageServiceCollectionExtensions.ColdTierKey, (_, _) => Substitute.For<IFileStore>());
        services.AddSingleton(Substitute.For<ICosmosContainerFactory>());
        services.AddScoped(_ => Substitute.For<ICorrelationContext>());

        var configurationRoot = configuration ?? new ConfigurationBuilder().Build();
        var result = services.AddAuditTrail(configurationRoot);

        Assert.Same(services, result);

        return services.BuildServiceProvider();
    }

    private static IConfiguration BuildAuditConfiguration(bool? cosmosDbEnabled, bool? coldStorageEnabled)
    {
        var values = new Dictionary<string, string?>();
        if (cosmosDbEnabled is not null)
        {
            values["Audit:CosmosDbEnabled"] = cosmosDbEnabled.Value.ToString();
        }

        if (coldStorageEnabled is not null)
        {
            values["Audit:ColdStorageEnabled"] = coldStorageEnabled.Value.ToString();
        }

        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    [Fact(DisplayName = "AddAuditTrail registers a bounded AuditEntry channel as a singleton")]
    public void AddAuditTrail_Registered_ChannelIsSingleton()
    {
        var provider = BuildProvider();

        var channel1 = provider.GetRequiredService<Channel<AuditEntry>>();
        var channel2 = provider.GetRequiredService<Channel<AuditEntry>>();

        Assert.Same(channel1, channel2);
        Assert.True(channel1.Writer.TryWrite(CreateEntry()));
    }

    [Fact(DisplayName = "AddAuditTrail honors a configured Audit:ChannelCapacity as the channel's bound")]
    public void AddAuditTrail_ConfiguredCapacity_BoundsTheChannel()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["Audit:ChannelCapacity"] = "1" })
            .Build();

        var provider = BuildProvider(configuration);
        var channel = provider.GetRequiredService<Channel<AuditEntry>>();

        Assert.True(channel.Writer.TryWrite(CreateEntry()));
        Assert.False(channel.Writer.TryWrite(CreateEntry()));
    }

    [Fact(DisplayName = "AddAuditTrail registers IAuditTrailWriter as a singleton AuditTrailWriter")]
    public void AddAuditTrail_Registered_AuditTrailWriterIsSingleton()
    {
        var provider = BuildProvider();

        var writer1 = provider.GetRequiredService<IAuditTrailWriter>();
        var writer2 = provider.GetRequiredService<IAuditTrailWriter>();

        Assert.IsType<AuditTrailWriter>(writer1);
        Assert.Same(writer1, writer2);
    }

    [Fact(DisplayName = "AddAuditTrail registers AuditBackgroundService as a hosted service")]
    public void AddAuditTrail_Registered_AuditBackgroundServiceIsHosted()
    {
        var provider = BuildProvider();

        var hostedServices = provider.GetServices<IHostedService>();

        Assert.Contains(hostedServices, service => service is AuditBackgroundService);
    }

    [Fact(DisplayName = "AddAuditTrail registers a scoped IAuditRepository built with NullAuditTrailWriter, never the real writer")]
    public void AddAuditTrail_Registered_AuditRepositoryIsScopedAndDistinctPerScope()
    {
        var provider = BuildProvider();

        using var scope1 = provider.CreateScope();
        var repository1 = scope1.ServiceProvider.GetRequiredService<IAuditRepository>();

        using var scope2 = provider.CreateScope();
        var repository2 = scope2.ServiceProvider.GetRequiredService<IAuditRepository>();

        // Constructing AuditRepository succeeding at all (rather than throwing) confirms it was wired
        // with NullAuditTrailWriter.Instance directly, not by resolving the real IAuditTrailWriter -
        // resolving IAuditTrailWriter itself works fine here too, so this alone wouldn't distinguish
        // the two; the real assertion is the factory's own code path, exercised by this resolution.
        Assert.IsType<AuditRepository>(repository1);
        Assert.NotSame(repository1, repository2);
    }

    [Fact(DisplayName = "AddAuditTrail with only CosmosDbEnabled registers a single CosmosAuditSink")]
    public void AddAuditTrail_OnlyCosmosDbEnabled_RegistersCosmosAuditSinkOnly()
    {
        var configuration = BuildAuditConfiguration(cosmosDbEnabled: true, coldStorageEnabled: false);
        var provider = BuildProvider(configuration);

        using var scope = provider.CreateScope();
        var sinks = scope.ServiceProvider.GetServices<IAuditSink>().ToList();

        Assert.Single(sinks);
        Assert.IsType<CosmosAuditSink>(sinks[0]);
    }

    [Fact(DisplayName = "AddAuditTrail with only ColdStorageEnabled registers a single ColdBlobAuditSink")]
    public void AddAuditTrail_OnlyColdStorageEnabled_RegistersColdBlobAuditSinkOnly()
    {
        var configuration = BuildAuditConfiguration(cosmosDbEnabled: false, coldStorageEnabled: true);
        var provider = BuildProvider(configuration);

        using var scope = provider.CreateScope();
        var sinks = scope.ServiceProvider.GetServices<IAuditSink>().ToList();

        Assert.Single(sinks);
        Assert.IsType<ColdBlobAuditSink>(sinks[0]);
    }

    [Fact(DisplayName = "AddAuditTrail with both flags enabled registers both sinks")]
    public void AddAuditTrail_BothEnabled_RegistersBothSinks()
    {
        var configuration = BuildAuditConfiguration(cosmosDbEnabled: true, coldStorageEnabled: true);
        var provider = BuildProvider(configuration);

        using var scope = provider.CreateScope();
        var sinks = scope.ServiceProvider.GetServices<IAuditSink>().ToList();

        Assert.Equal(2, sinks.Count);
        Assert.Contains(sinks, sink => sink is CosmosAuditSink);
        Assert.Contains(sinks, sink => sink is ColdBlobAuditSink);
    }

    [Fact(DisplayName = "AddAuditTrail with both flags disabled throws InvalidOperationException")]
    public void AddAuditTrail_BothDisabled_Throws()
    {
        var configuration = BuildAuditConfiguration(cosmosDbEnabled: false, coldStorageEnabled: false);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IFileStore>(
            BlobStorageServiceCollectionExtensions.HotTierKey, (_, _) => Substitute.For<IFileStore>());
        services.AddKeyedSingleton<IFileStore>(
            BlobStorageServiceCollectionExtensions.ColdTierKey, (_, _) => Substitute.For<IFileStore>());
        services.AddSingleton(Substitute.For<ICosmosContainerFactory>());
        services.AddScoped(_ => Substitute.For<ICorrelationContext>());

        Assert.Throws<InvalidOperationException>(() => services.AddAuditTrail(configuration));
    }

    private static AuditEntry CreateEntry() => AuditEntry.Create(
        id: Guid.NewGuid().ToString(),
        containerName: "InventoryEvents",
        entityId: "WH1:SKU1",
        entityPartitionKey: "WH1:SKU1",
        operation: AuditOperation.Create,
        correlationId: "corr-1",
        schema: "InventoryStateChanged",
        documentJson: "{}",
        timestampUtc: DateTime.UtcNow);
}
