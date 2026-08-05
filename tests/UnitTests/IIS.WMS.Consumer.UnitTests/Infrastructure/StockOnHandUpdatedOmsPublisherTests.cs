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
/// §7.3/§9 B2C stock notification publish tests for <see cref="StockOnHandUpdatedOmsPublisher"/>
/// (docs/events/inventory.StockOnHandUpdated.md), with the relay publisher, fulfilment unit
/// repository, and country repository mocked.
/// </summary>
public class StockOnHandUpdatedOmsPublisherTests
{
    private sealed class RecordingLogger : ILogger<StockOnHandUpdatedOmsPublisher>
    {
        public List<LogLevel> Levels { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Levels.Add(logLevel);
    }

    private readonly IFulfilmentUnitRepository fulfilmentUnitRepository = Substitute.For<IFulfilmentUnitRepository>();
    private readonly ICountryRepository countryRepository = Substitute.For<ICountryRepository>();
    private readonly IServiceBusRelayPublisher relayPublisher = Substitute.For<IServiceBusRelayPublisher>();
    private readonly ICorrelationContext correlationContext = Substitute.For<ICorrelationContext>();
    private readonly RecordingLogger logger = new();
    private readonly StockOnHandUpdatedOmsPublisher sut;

    private static readonly StockOnHandUpdatedOmsQuantityDetail[] QuantityDetails =
    [
        new() { Quantity = 5, State = "AVAILABLE", Status = "PICKABLE", CountryOfOrigin = "TH", Hallmarking = "NON" },
    ];

    public StockOnHandUpdatedOmsPublisherTests()
    {
        var publishOptions = Substitute.For<IOptions<InventoryPublishOptions>>();
        publishOptions.Value.Returns(new InventoryPublishOptions { IcrSnapshotQueueName = "nexus-producer" });
        countryRepository.GetByCodeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new CountryMaster { Code = "TH", Name = "Thailand", RegionCode = "APAC", IsActive = true });

        sut = new StockOnHandUpdatedOmsPublisher(
            fulfilmentUnitRepository, countryRepository, relayPublisher, publishOptions, correlationContext, logger);
    }

    [Fact(DisplayName = "PublishAsync resolves Market from the fulfilment unit's CountryCode when found")]
    public async Task PublishAsync_FulfilmentUnitFound_ResolvesMarketFromCountryCode()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("BRZDC3PL", Arg.Any<CancellationToken>())
            .Returns(new FulfilmentUnit { FulfilmentId = "BRZDC3PL", CountryCode = "TH" });
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "BRZDC3PL", "THIRD_PARTY_LOGISTICS", "SKU1", "EA", "ORG-1", "1234567890",
            QuantityDetails, "RECEIPT", new DateTime(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc), "EVT-1",
            CancellationToken.None);

        var request = JsonSerializer.Deserialize<StockOnHandUpdatedOmsPublishRequest>(captured!.Json)!;
        Assert.Equal("TH", request.Market);
    }

    [Fact(DisplayName = "PublishAsync falls back to UNKNOWN Market when no fulfilment unit is found")]
    public async Task PublishAsync_FulfilmentUnitNotFound_FallsBackToUnknownMarket()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("BRZDC3PL", Arg.Any<CancellationToken>())
            .Returns((FulfilmentUnit?)null);
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "BRZDC3PL", "THIRD_PARTY_LOGISTICS", "SKU1", "EA", "ORG-1", "1234567890",
            QuantityDetails, "RECEIPT", new DateTime(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc), "EVT-1",
            CancellationToken.None);

        var request = JsonSerializer.Deserialize<StockOnHandUpdatedOmsPublishRequest>(captured!.Json)!;
        Assert.Equal("UNKNOWN", request.Market);
    }

    [Fact(DisplayName = "PublishAsync fixes Channel to OWN_ONLINE regardless of the inbound event's own channel")]
    public async Task PublishAsync_Always_FixesChannelToOwnOnline()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("BRZDC3PL", Arg.Any<CancellationToken>())
            .Returns((FulfilmentUnit?)null);
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "BRZDC3PL", "THIRD_PARTY_LOGISTICS", "SKU1", "EA", "ORG-1", "1234567890",
            QuantityDetails, "RECEIPT", new DateTime(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc), "EVT-1",
            CancellationToken.None);

        var request = JsonSerializer.Deserialize<StockOnHandUpdatedOmsPublishRequest>(captured!.Json)!;
        Assert.Equal("OWN_ONLINE", request.Channel);
    }

    [Fact(DisplayName = "PublishAsync populates request fields from the inbound inputs, including the location and quantity details")]
    public async Task PublishAsync_Always_PopulatesRequestFieldsFromInputs()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("BRZDC3PL", Arg.Any<CancellationToken>())
            .Returns((FulfilmentUnit?)null);
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);
        var updatedDate = new DateTime(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc);

        await sut.PublishAsync(
            "BRZDC3PL", "THIRD_PARTY_LOGISTICS", "SKU1", "EA", "ORG-1", "1234567890",
            QuantityDetails, "RECEIPT", updatedDate, "EVT-1", CancellationToken.None);

        var request = JsonSerializer.Deserialize<StockOnHandUpdatedOmsPublishRequest>(captured!.Json)!;
        Assert.Equal("SKU1", request.ProductId);
        Assert.Equal("EA", request.ProductUnits);
        Assert.Equal("BRZDC3PL", request.Location.Id);
        Assert.Equal("THIRD_PARTY_LOGISTICS", request.Location.Type);
        Assert.Equal("ORG-1", request.Entity);
        Assert.Equal("1234567890", request.Barcode);
        Assert.Equal("RECEIPT", request.Reason);
        Assert.Equal(updatedDate, request.UpdatedDate);
        Assert.Single(request.QuantityDetails);
        Assert.Equal(5, request.QuantityDetails[0].Quantity);
        Assert.Equal("TH", request.QuantityDetails[0].CountryOfOrigin);
        Assert.Equal("NON", request.QuantityDetails[0].Hallmarking);
    }

    [Fact(DisplayName = "PublishAsync relays onto the configured IcrSnapshotQueueName queue with the expected event type and a deterministic message id")]
    public async Task PublishAsync_Always_RelaysToConfiguredQueueWithExpectedType()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("BRZDC3PL", Arg.Any<CancellationToken>())
            .Returns((FulfilmentUnit?)null);
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "BRZDC3PL", "THIRD_PARTY_LOGISTICS", "SKU1", "EA", "ORG-1", "1234567890",
            QuantityDetails, "RECEIPT", new DateTime(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc), "EVT-1",
            CancellationToken.None);

        Assert.Equal("nexus-producer", captured!.QueueName);
        Assert.Equal(["Inventory_B2CStockOnHandUpdated"], captured.Types);
        Assert.Equal("BRZDC3PL:SKU1:EVT-1", captured.SessionId);
        Assert.Equal("BRZDC3PL:SKU1:EVT-1:Inventory_B2CStockOnHandUpdated", captured.MessageId);
    }

    [Fact(DisplayName = "PublishAsync logs a warning but still publishes the resolved Market when the CountryMaster is missing or inactive")]
    public async Task PublishAsync_MarketResolvedButCountryMasterMissing_LogsWarningWithoutChangingMarket()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("BRZDC3PL", Arg.Any<CancellationToken>())
            .Returns(new FulfilmentUnit { FulfilmentId = "BRZDC3PL", CountryCode = "TH" });
        countryRepository.GetByCodeAsync("TH", Arg.Any<CancellationToken>())
            .Returns((CountryMaster?)null);
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "BRZDC3PL", "THIRD_PARTY_LOGISTICS", "SKU1", "EA", "ORG-1", "1234567890",
            QuantityDetails, "RECEIPT", new DateTime(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc), "EVT-1",
            CancellationToken.None);

        var request = JsonSerializer.Deserialize<StockOnHandUpdatedOmsPublishRequest>(captured!.Json)!;
        Assert.Equal("TH", request.Market);
        Assert.Contains(LogLevel.Warning, logger.Levels);
    }

    [Fact(DisplayName = "PublishAsync skips the CountryRepository lookup when Market falls back to UNKNOWN")]
    public async Task PublishAsync_MarketUnknown_SkipsCountryLookup()
    {
        fulfilmentUnitRepository.GetByFulfilmentIdAsync("BRZDC3PL", Arg.Any<CancellationToken>())
            .Returns((FulfilmentUnit?)null);
        relayPublisher.PublishAsync(Arg.Any<ServiceBusRelayMessage>(), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "BRZDC3PL", "THIRD_PARTY_LOGISTICS", "SKU1", "EA", "ORG-1", "1234567890",
            QuantityDetails, "RECEIPT", new DateTime(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc), "EVT-1",
            CancellationToken.None);

        await countryRepository.DidNotReceive().GetByCodeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}
