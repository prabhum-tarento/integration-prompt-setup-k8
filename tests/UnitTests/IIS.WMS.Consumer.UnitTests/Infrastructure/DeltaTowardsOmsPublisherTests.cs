using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Egress;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// §3.7 OMS delta publish tests for <see cref="DeltaTowardsOmsPublisher"/>
/// (docs/events/shared/delta-towards-oms.md), with the relay publisher,
/// fulfilment unit repository, and country repository mocked.
/// </summary>
public class DeltaTowardsOmsPublisherTests
{
    private sealed class RecordingLogger : ILogger<DeltaTowardsOmsPublisher>
    {
        public List<LogLevel> Levels { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Levels.Add(logLevel);
    }

    private static readonly DateTimeOffset Now = new(2026, 7, 8, 12, 0, 0, TimeSpan.Zero);

    private readonly IFulfilmentUnitRepository fulfilmentUnitRepository = Substitute.For<IFulfilmentUnitRepository>();
    private readonly ICountryRepository countryRepository = Substitute.For<ICountryRepository>();
    private readonly IServiceBusRelayPublisher relayPublisher = Substitute.For<IServiceBusRelayPublisher>();
    private readonly ICorrelationContext correlationContext = Substitute.For<ICorrelationContext>();
    private readonly TimeProvider timeProvider = Substitute.For<TimeProvider>();
    private readonly RecordingLogger logger = new();
    private readonly DeltaTowardsOmsPublisher sut;

    public DeltaTowardsOmsPublisherTests()
    {
        timeProvider.GetUtcNow().Returns(Now);
        var publishOptions = Substitute.For<IOptions<InventoryPublishOptions>>();
        publishOptions.Value.Returns(new InventoryPublishOptions { OmsDeltaQueueName = "oms-queue" });
        countryRepository.GetByCodeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new CountryMaster { Code = "TH", Name = "Thailand", RegionCode = "APAC", IsActive = true });

        sut = new DeltaTowardsOmsPublisher(
            fulfilmentUnitRepository, countryRepository, relayPublisher, publishOptions, correlationContext,
            timeProvider, logger);
    }

    [Fact(DisplayName = "PublishAsync resolves Market from the fulfilment unit's CountryCode when found")]
    public async Task PublishAsync_FulfilmentUnitFound_ResolvesMarketFromCountryCode()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("WH1", Arg.Any<CancellationToken>())
            .Returns(new FulfilmentUnit { FulfilmentId = "WH1", CountryCode = "TH" });
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("SKU1", "WH1", "Warehouse", "TH", "925", 260, "EVT-1", CancellationToken.None);

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

        await sut.PublishAsync("SKU1", "WH1", "Warehouse", "TH", "925", 260, "EVT-1", CancellationToken.None);

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

        await sut.PublishAsync("SKU1", "WH1", "Warehouse", "TH", "925", 260, "EVT-1", CancellationToken.None);

        var request = JsonSerializer.Deserialize<DeltaTowardsOmsPublishRequest>(captured!.Json)!;
        Assert.Equal("WH1:SKU1:EVT-1", request.ReferenceId);
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

        await sut.PublishAsync("SKU1", "WH1", "Warehouse", "TH", "925", 260, "EVT-1", CancellationToken.None);

        Assert.Equal("oms-queue", captured!.QueueName);
        Assert.Equal(["Inventory_B2CInventoryAdjusted"], captured.Types);
    }

    [Fact(DisplayName = "PublishAsync logs a warning but still publishes the resolved Market when the CountryMaster is missing or inactive")]
    public async Task PublishAsync_MarketResolvedButCountryMasterMissing_LogsWarningWithoutChangingMarket()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("WH1", Arg.Any<CancellationToken>())
            .Returns(new FulfilmentUnit { FulfilmentId = "WH1", CountryCode = "TH" });
        countryRepository.GetByCodeAsync("TH", Arg.Any<CancellationToken>())
            .Returns((CountryMaster?)null);
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("SKU1", "WH1", "Warehouse", "TH", "925", 260, "EVT-1", CancellationToken.None);

        var request = JsonSerializer.Deserialize<DeltaTowardsOmsPublishRequest>(captured!.Json)!;
        Assert.Equal("TH", request.Market);
        Assert.Contains(LogLevel.Warning, logger.Levels);
    }

    [Fact(DisplayName = "PublishAsync skips the CountryRepository lookup when Market falls back to UNKNOWN")]
    public async Task PublishAsync_MarketUnknown_SkipsCountryLookup()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("WH1", Arg.Any<CancellationToken>())
            .Returns((FulfilmentUnit?)null);
        relayPublisher.PublishAsync(Arg.Any<ServiceBusRelayMessage>(), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync("SKU1", "WH1", "Warehouse", "TH", "925", 260, "EVT-1", CancellationToken.None);

        await countryRepository.DidNotReceive().GetByCodeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}
