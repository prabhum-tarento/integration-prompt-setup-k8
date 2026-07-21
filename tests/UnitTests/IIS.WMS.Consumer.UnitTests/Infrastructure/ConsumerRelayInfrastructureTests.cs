using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.OrderArchiving;
using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="ConsumerRelayInfrastructure"/> - a plain facade bundling the
/// dependencies every <see cref="KafkaConsumerHostedServiceBase"/> subclass needs, so these only need to prove
/// each constructor argument surfaces unchanged on the matching property (integration-resiliency
/// .instructions.md §1's "cut constructor over-injection" rationale).
/// </summary>
public class ConsumerRelayInfrastructureTests
{
    [Fact(DisplayName = "The constructor exposes every dependency unchanged on its matching property")]
    public void Constructor_GivenDependencies_ExposesEachOnMatchingProperty()
    {
        var relayPublisher = Substitute.For<IServiceBusRelayPublisher>();
        var hotFileStore = Substitute.For<IFileStore>();
        var coldFileStore = Substitute.For<IFileStore>();
        var blobStorageOptions = Options.Create(new BlobStorageOptions { RequestAuditContainerName = "audit-1" });
        var deduplicationService = Substitute.For<IDeduplicationService>();
        var dynamicEventValidator = Substitute.For<IDynamicEventValidator>();
        var orderArchiveWriter = Substitute.For<IOrderArchiveWriter>();
        var services = new ServiceCollection();
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
        var applicationOptions = Options.Create(new ApplicationOptions { AppName = "IIS.WMS.Consumer", AppId = "app-1" });

        var sut = new ConsumerRelayInfrastructure(
            relayPublisher, hotFileStore, coldFileStore, blobStorageOptions,
            deduplicationService, dynamicEventValidator, orderArchiveWriter, scopeFactory, applicationOptions);

        Assert.Same(relayPublisher, sut.RelayPublisher);
        Assert.Same(hotFileStore, sut.HotFileStore);
        Assert.Same(coldFileStore, sut.ColdFileStore);
        Assert.Same(blobStorageOptions, sut.BlobStorageOptions);
        Assert.Same(deduplicationService, sut.DeduplicationService);
        Assert.Same(dynamicEventValidator, sut.DynamicEventValidator);
        Assert.Same(orderArchiveWriter, sut.OrderArchiveWriter);
        Assert.Same(scopeFactory, sut.ScopeFactory);
        Assert.Equal("app-1", sut.ApplicationOptions.AppId);
        Assert.Equal("IIS.WMS.Consumer", sut.ApplicationOptions.AppName);
    }
}
