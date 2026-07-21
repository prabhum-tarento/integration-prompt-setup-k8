using System.Threading.Channels;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Registration tests for <see cref="MessageArchiveServiceCollectionExtensions.AddMessageArchiving"/> -
/// the bounded MessageArchive channel, <see cref="IMessageArchiveWriter"/>,
/// <see cref="MessageArchiveBackgroundService"/>, and the configured <see cref="IMessageArchiveSink"/>
/// destination(s). Mirrors <c>AuditServiceCollectionExtensionsTests</c>, with one deliberately opposite
/// case: <see cref="AddMessageArchiving_BothDisabled_DoesNotThrowAndRegistersNoSinks"/> asserts no throw
/// and zero sink registrations, the exact opposite of <c>AuditServiceCollectionExtensionsTests.AddAuditTrail_BothDisabled_Throws</c>.
/// </summary>
public class MessageArchiveServiceCollectionExtensionsTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    private static IServiceProvider BuildProvider(IConfiguration? configuration = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IFileStore>(
            BlobStorageServiceCollectionExtensions.HotTierKey, (_, _) => Substitute.For<IFileStore>());
        services.AddKeyedSingleton<IFileStore>(
            BlobStorageServiceCollectionExtensions.ColdTierKey, (_, _) => Substitute.For<IFileStore>());
        services.AddScoped(_ => Substitute.For<IMessageArchiveRepository>());

        var configurationRoot = configuration ?? new ConfigurationBuilder().Build();
        var result = services.AddMessageArchiving(configurationRoot);

        Assert.Same(services, result);

        return services.BuildServiceProvider();
    }

    private static IConfiguration BuildMessageArchiveConfiguration(bool? cosmosDbEnabled, bool? blobEnabled)
    {
        var values = new Dictionary<string, string?>();
        if (cosmosDbEnabled is not null)
        {
            values["MessageArchive:CosmosDbEnabled"] = cosmosDbEnabled.Value.ToString();
        }

        if (blobEnabled is not null)
        {
            values["MessageArchive:BlobEnabled"] = blobEnabled.Value.ToString();
        }

        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    [Fact(DisplayName = "AddMessageArchiving registers a bounded MessageArchive channel as a singleton")]
    public void AddMessageArchiving_Registered_ChannelIsSingleton()
    {
        var provider = BuildProvider();

        var channel1 = provider.GetRequiredService<Channel<MessageArchive>>();
        var channel2 = provider.GetRequiredService<Channel<MessageArchive>>();

        Assert.Same(channel1, channel2);
        Assert.True(channel1.Writer.TryWrite(CreateEntry()));
    }

    [Fact(DisplayName = "AddMessageArchiving honors a configured MessageArchive:ChannelCapacity as the channel's bound")]
    public void AddMessageArchiving_ConfiguredCapacity_BoundsTheChannel()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["MessageArchive:ChannelCapacity"] = "1" })
            .Build();

        var provider = BuildProvider(configuration);
        var channel = provider.GetRequiredService<Channel<MessageArchive>>();

        Assert.True(channel.Writer.TryWrite(CreateEntry()));
        Assert.False(channel.Writer.TryWrite(CreateEntry()));
    }

    [Fact(DisplayName = "AddMessageArchiving registers IMessageArchiveWriter as a singleton MessageArchiveWriter")]
    public void AddMessageArchiving_Registered_MessageArchiveWriterIsSingleton()
    {
        var provider = BuildProvider();

        var writer1 = provider.GetRequiredService<IMessageArchiveWriter>();
        var writer2 = provider.GetRequiredService<IMessageArchiveWriter>();

        Assert.IsType<MessageArchiveWriter>(writer1);
        Assert.Same(writer1, writer2);
    }

    [Fact(DisplayName = "AddMessageArchiving registers MessageArchiveBackgroundService as a hosted service")]
    public void AddMessageArchiving_Registered_MessageArchiveBackgroundServiceIsHosted()
    {
        var provider = BuildProvider();

        var hostedServices = provider.GetServices<IHostedService>();

        Assert.Contains(hostedServices, service => service is MessageArchiveBackgroundService);
    }

    [Fact(DisplayName = "AddMessageArchiving with only CosmosDbEnabled registers a single CosmosMessageArchiveSink")]
    public void AddMessageArchiving_OnlyCosmosDbEnabled_RegistersCosmosMessageArchiveSinkOnly()
    {
        var configuration = BuildMessageArchiveConfiguration(cosmosDbEnabled: true, blobEnabled: false);
        var provider = BuildProvider(configuration);

        using var scope = provider.CreateScope();
        var sinks = scope.ServiceProvider.GetServices<IMessageArchiveSink>().ToList();

        Assert.Single(sinks);
        Assert.IsType<CosmosMessageArchiveSink>(sinks[0]);
    }

    [Fact(DisplayName = "AddMessageArchiving with only BlobEnabled registers a single BlobMessageArchiveSink")]
    public void AddMessageArchiving_OnlyBlobEnabled_RegistersBlobMessageArchiveSinkOnly()
    {
        var configuration = BuildMessageArchiveConfiguration(cosmosDbEnabled: false, blobEnabled: true);
        var provider = BuildProvider(configuration);

        using var scope = provider.CreateScope();
        var sinks = scope.ServiceProvider.GetServices<IMessageArchiveSink>().ToList();

        Assert.Single(sinks);
        Assert.IsType<BlobMessageArchiveSink>(sinks[0]);
    }

    [Fact(DisplayName = "AddMessageArchiving with both flags enabled registers both sinks")]
    public void AddMessageArchiving_BothEnabled_RegistersBothSinks()
    {
        var configuration = BuildMessageArchiveConfiguration(cosmosDbEnabled: true, blobEnabled: true);
        var provider = BuildProvider(configuration);

        using var scope = provider.CreateScope();
        var sinks = scope.ServiceProvider.GetServices<IMessageArchiveSink>().ToList();

        Assert.Equal(2, sinks.Count);
        Assert.Contains(sinks, sink => sink is CosmosMessageArchiveSink);
        Assert.Contains(sinks, sink => sink is BlobMessageArchiveSink);
    }

    [Fact(DisplayName = "AddMessageArchiving with both flags disabled does not throw and registers zero sinks")]
    public void AddMessageArchiving_BothDisabled_DoesNotThrowAndRegistersNoSinks()
    {
        var configuration = BuildMessageArchiveConfiguration(cosmosDbEnabled: false, blobEnabled: false);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IFileStore>(
            BlobStorageServiceCollectionExtensions.HotTierKey, (_, _) => Substitute.For<IFileStore>());
        services.AddKeyedSingleton<IFileStore>(
            BlobStorageServiceCollectionExtensions.ColdTierKey, (_, _) => Substitute.For<IFileStore>());
        services.AddScoped(_ => Substitute.For<IMessageArchiveRepository>());

        var exception = Record.Exception(() => services.AddMessageArchiving(configuration));
        Assert.Null(exception);

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var sinks = scope.ServiceProvider.GetServices<IMessageArchiveSink>().ToList();

        Assert.Empty(sinks);
    }

    private static MessageArchive CreateEntry() => MessageArchive.Create(
        id: "InventoryStateChanged_corr-1",
        category: "InventoryStateChanged",
        payload: "{}",
        correlationId: "corr-1",
        timestamp: Now);
}
