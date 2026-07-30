using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// §3.8 ICR snapshot publish tests for <see cref="InventoryComparisonReportPublisher"/>
/// (docs/InventoryStateChangedFullQueueTrigger.md), with the repository and relay publisher mocked.
/// </summary>
public class InventoryComparisonReportPublisherTests
{
    private static readonly DateTimeOffset Now = new(2026, 7, 8, 12, 0, 0, TimeSpan.Zero);
    private static readonly string Id = ItemStockInventory.BuildId("WH1", "SKU1", "925", "TH");

    private readonly IItemStockInventoryRepository repository = Substitute.For<IItemStockInventoryRepository>();
    private readonly IServiceBusRelayPublisher relayPublisher = Substitute.For<IServiceBusRelayPublisher>();
    private readonly ICorrelationContext correlationContext = Substitute.For<ICorrelationContext>();
    private readonly TimeProvider timeProvider = Substitute.For<TimeProvider>();
    private readonly InventoryComparisonReportPublisher sut;

    public InventoryComparisonReportPublisherTests()
    {
        timeProvider.GetUtcNow().Returns(Now);
        var publishOptions = Substitute.For<IOptions<InventoryPublishOptions>>();
        publishOptions.Value.Returns(new InventoryPublishOptions { IcrSnapshotQueueName = "icr-queue" });

        sut = new InventoryComparisonReportPublisher(
            repository, relayPublisher, publishOptions, correlationContext, timeProvider,
            NullLogger<InventoryComparisonReportPublisher>.Instance);
    }

    private static ItemStockInventory CreateAggregate(
        bool isExtended = false, int b2cOriginal = 55, int b2cAvailable = 320,
        int b2bAvailable = 500, int b2bPrepared = 10, int b2cPrepared = 20) =>
        ItemStockInventory.Rehydrate(
            Id, "WH1", "SKU1", "TH", "925",
            b2bAvailable: b2bAvailable, b2cAvailable: b2cAvailable, b2cOriginal: b2cOriginal, b2cExtended: 0,
            b2cAllocated: 0, b2bAllocated: 0, b2cPrepared: b2cPrepared, b2bPrepared: b2bPrepared,
            internalHallmarkAllocated: 0, inTransit: 0, b2cThreshold: 0, isExtended: isExtended, b2bUsedShare: 0,
            inspection: 0, psc: 0, isPosm: false, modifiedUtc: Now.UtcDateTime);

    [Fact(DisplayName = "PublishAsync skips the publish when no ItemStockInventory record is found")]
    public async Task PublishAsync_NoRecordFound_SkipsPublish()
    {
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns((ItemStockInventory?)null);

        await sut.PublishAsync("WH1", "SKU1", "925", "TH", isThirdPartyLogistics: false, CancellationToken.None);

        await relayPublisher.DidNotReceiveWithAnyArgs().PublishAsync(default!, default);
    }

    [Fact(DisplayName = "PublishAsync uses B2CAvailable for the B2C_AVL detail when the inventory is not extended")]
    public async Task PublishAsync_NotExtended_UsesB2CAvailableForB2CDetail()
    {
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(CreateAggregate(isExtended: false, b2cAvailable: 320, b2cOriginal: 55));
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("WH1", "SKU1", "925", "TH", isThirdPartyLogistics: false, CancellationToken.None);

        var request = JsonSerializer.Deserialize<OmniInventoryAvailabilityPublishRequest>(captured!.Json)!;
        var b2cAvl = request.QuantityDetails.Single(d => d.Domain == "B2C" && d.Status == "PICKABLE");
        Assert.Equal(320, b2cAvl.Quantity);
    }

    [Fact(DisplayName = "PublishAsync uses B2COriginal for the B2C_AVL detail when the inventory is extended")]
    public async Task PublishAsync_Extended_UsesB2COriginalForB2CDetail()
    {
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(CreateAggregate(isExtended: true, b2cAvailable: 320, b2cOriginal: 55));
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("WH1", "SKU1", "925", "TH", isThirdPartyLogistics: false, CancellationToken.None);

        var request = JsonSerializer.Deserialize<OmniInventoryAvailabilityPublishRequest>(captured!.Json)!;
        var b2cAvl = request.QuantityDetails.Single(d => d.Domain == "B2C" && d.Status == "PICKABLE");
        Assert.Equal(55, b2cAvl.Quantity);
    }

    [Fact(DisplayName = "PublishAsync builds all four quantity details with the expected quantities, states, statuses and domains")]
    public async Task PublishAsync_Always_BuildsAllFourQuantityDetails()
    {
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>())
            .Returns(CreateAggregate(isExtended: false, b2bAvailable: 500, b2cAvailable: 320, b2bPrepared: 10, b2cPrepared: 20));
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("WH1", "SKU1", "925", "TH", isThirdPartyLogistics: false, CancellationToken.None);

        var request = JsonSerializer.Deserialize<OmniInventoryAvailabilityPublishRequest>(captured!.Json)!;
        Assert.Equal(4, request.QuantityDetails.Count);
        Assert.Equal(500, request.QuantityDetails.Single(d => d is { Domain: "B2B", Status: "PICKABLE" }).Quantity);
        Assert.Equal(320, request.QuantityDetails.Single(d => d is { Domain: "B2C", Status: "PICKABLE" }).Quantity);
        Assert.Equal(10, request.QuantityDetails.Single(d => d is { Domain: "B2B", Status: "PREPARED" }).Quantity);
        Assert.Equal(20, request.QuantityDetails.Single(d => d is { Domain: "B2C", Status: "PREPARED" }).Quantity);
        Assert.All(request.QuantityDetails, d => Assert.Equal("AVAILABLE", d.State));
    }

    [Theory(DisplayName = "PublishAsync sets Location.Type based on isThirdPartyLogistics")]
    [InlineData(true, "ThirdPartyLogistics")]
    [InlineData(false, "Warehouse")]
    public async Task PublishAsync_LocationTypeReflectsThirdPartyLogisticsFlag(bool isThirdPartyLogistics, string expectedType)
    {
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(CreateAggregate());
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("WH1", "SKU1", "925", "TH", isThirdPartyLogistics, CancellationToken.None);

        var request = JsonSerializer.Deserialize<OmniInventoryAvailabilityPublishRequest>(captured!.Json)!;
        Assert.Equal(expectedType, request.Location.Type);
    }

    [Fact(DisplayName = "PublishAsync relays onto the configured ICR snapshot queue with the expected event type")]
    public async Task PublishAsync_Always_RelaysToConfiguredQueueWithExpectedType()
    {
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(CreateAggregate());
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("WH1", "SKU1", "925", "TH", isThirdPartyLogistics: false, CancellationToken.None);

        Assert.Equal("icr-queue", captured!.QueueName);
        Assert.Equal(["Inventory_OmniInventoryAvailabilityReported"], captured.Types);
    }
}
