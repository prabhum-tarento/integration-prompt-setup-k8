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
/// §3.7 OMS delta publish tests for <see cref="DeltaTowardsOmsPublisher"/>
/// (docs/InventoryStateChangedFullQueueTrigger.md), with the relay publisher and
/// fulfilment unit repository mocked.
/// </summary>
public class DeltaTowardsOmsPublisherTests
{
    private static readonly DateTimeOffset Now = new(2026, 7, 8, 12, 0, 0, TimeSpan.Zero);

    private readonly IFulfilmentUnitRepository fulfilmentUnitRepository = Substitute.For<IFulfilmentUnitRepository>();
    private readonly IServiceBusRelayPublisher relayPublisher = Substitute.For<IServiceBusRelayPublisher>();
    private readonly ICorrelationContext correlationContext = Substitute.For<ICorrelationContext>();
    private readonly TimeProvider timeProvider = Substitute.For<TimeProvider>();
    private readonly DeltaTowardsOmsPublisher sut;

    public DeltaTowardsOmsPublisherTests()
    {
        timeProvider.GetUtcNow().Returns(Now);
        var publishOptions = Substitute.For<IOptions<InventoryPublishOptions>>();
        publishOptions.Value.Returns(new InventoryPublishOptions { OmsDeltaQueueName = "oms-queue" });

        sut = new DeltaTowardsOmsPublisher(
            fulfilmentUnitRepository, relayPublisher, publishOptions, correlationContext, timeProvider,
            NullLogger<DeltaTowardsOmsPublisher>.Instance);
    }

    [Fact(DisplayName = "PublishAsync resolves Market from the fulfilment unit's CountryCode when found")]
    public async Task PublishAsync_FulfilmentUnitFound_ResolvesMarketFromCountryCode()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("WH1", Arg.Any<CancellationToken>())
            .Returns(new FulfilmentUnit { FulfilmentId = "WH1", CountryCode = "TH" });
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("SKU1", "WH1", "Warehouse", "TH", "925", 260, CancellationToken.None);

        var request = JsonSerializer.Deserialize<DeltaTowardsOmsPublishRequest>(captured!.Json)!;
        Assert.Equal("TH", request.Market);
    }

    [Fact(DisplayName = "PublishAsync falls back to UNKNOWN Market when no fulfilment unit is found")]
    public async Task PublishAsync_FulfilmentUnitNotFound_FallsBackToUnknownMarket()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("WH1", Arg.Any<CancellationToken>())
            .Returns((FulfilmentUnit?)null);
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("SKU1", "WH1", "Warehouse", "TH", "925", 260, CancellationToken.None);

        var request = JsonSerializer.Deserialize<DeltaTowardsOmsPublishRequest>(captured!.Json)!;
        Assert.Equal("UNKNOWN", request.Market);
    }

    [Fact(DisplayName = "PublishAsync populates ReferenceId, AdjustmentDate from TimeProvider, and the single quantity detail from the inbound delta")]
    public async Task PublishAsync_Always_PopulatesRequestFieldsFromInputs()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("WH1", Arg.Any<CancellationToken>())
            .Returns((FulfilmentUnit?)null);
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("SKU1", "WH1", "Warehouse", "TH", "925", 260, CancellationToken.None);

        var request = JsonSerializer.Deserialize<DeltaTowardsOmsPublishRequest>(captured!.Json)!;
        Assert.True(Guid.TryParse(request.ReferenceId, out _));
        Assert.Equal("SKU1", request.ProductId);
        Assert.Equal(Now.UtcDateTime, request.AdjustmentDate);
        Assert.Equal("ADJUSTMENT", request.Reason);
        Assert.Single(request.QuantityDetails);
        Assert.Equal(260, request.QuantityDetails[0].Quantity);
        Assert.Equal("TH", request.QuantityDetails[0].CountryOfOrigin);
        Assert.Equal("925", request.QuantityDetails[0].Hallmarking);
    }

    [Fact(DisplayName = "PublishAsync relays onto the configured OMS delta queue with the expected event type")]
    public async Task PublishAsync_Always_RelaysToConfiguredQueueWithExpectedType()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("WH1", Arg.Any<CancellationToken>())
            .Returns((FulfilmentUnit?)null);
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("SKU1", "WH1", "Warehouse", "TH", "925", 260, CancellationToken.None);

        Assert.Equal("oms-queue", captured!.QueueName);
        Assert.Equal(["Inventory_B2CInventoryAdjusted"], captured.Types);
    }
}
