using System.Threading.Channels;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Messaging.OrderArchiving;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Registration tests for <see cref="OrderArchiveServiceCollectionExtensions.AddOrderArchiving"/> - the
/// bounded OrderArchive channel, <see cref="IOrderArchiveWriter"/>, and
/// <see cref="OrderArchiveBackgroundService"/>.
/// </summary>
public class OrderArchiveServiceCollectionExtensionsTests
{
    private static IServiceProvider BuildProvider(IConfiguration? configuration = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddScoped(_ => Substitute.For<IOrderArchiveRepository>());

        var configurationRoot = configuration ?? new ConfigurationBuilder().Build();
        var result = services.AddOrderArchiving(configurationRoot);

        Assert.Same(services, result);

        return services.BuildServiceProvider();
    }

    [Fact(DisplayName = "AddOrderArchiving registers a bounded OrderArchive channel as a singleton")]
    public void AddOrderArchiving_Registered_ChannelIsSingleton()
    {
        var provider = BuildProvider();

        var channel1 = provider.GetRequiredService<Channel<OrderArchive>>();
        var channel2 = provider.GetRequiredService<Channel<OrderArchive>>();

        Assert.Same(channel1, channel2);
        Assert.True(channel1.Writer.TryWrite(CreateEntry()));
    }

    [Fact(DisplayName = "AddOrderArchiving honors a configured OrderArchive:ChannelCapacity as the channel's bound")]
    public void AddOrderArchiving_ConfiguredCapacity_BoundsTheChannel()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["OrderArchive:ChannelCapacity"] = "1" })
            .Build();

        var provider = BuildProvider(configuration);
        var channel = provider.GetRequiredService<Channel<OrderArchive>>();

        Assert.True(channel.Writer.TryWrite(CreateEntry()));
        Assert.False(channel.Writer.TryWrite(CreateEntry()));
    }

    [Fact(DisplayName = "AddOrderArchiving registers IOrderArchiveWriter as a singleton OrderArchiveWriter")]
    public void AddOrderArchiving_Registered_OrderArchiveWriterIsSingleton()
    {
        var provider = BuildProvider();

        var writer1 = provider.GetRequiredService<IOrderArchiveWriter>();
        var writer2 = provider.GetRequiredService<IOrderArchiveWriter>();

        Assert.IsType<OrderArchiveWriter>(writer1);
        Assert.Same(writer1, writer2);
    }

    [Fact(DisplayName = "AddOrderArchiving registers OrderArchiveBackgroundService as a hosted service")]
    public void AddOrderArchiving_Registered_OrderArchiveBackgroundServiceIsHosted()
    {
        var provider = BuildProvider();

        var hostedServices = provider.GetServices<IHostedService>();

        Assert.Contains(hostedServices, service => service is OrderArchiveBackgroundService);
    }

    private static OrderArchive CreateEntry() => OrderArchive.Create(
        id: Guid.NewGuid().ToString(),
        category: "WH1:SKU1",
        orderDetailJson: "{}",
        correlationId: "corr-1",
        timestamp: DateTime.UtcNow);
}
