using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Egress;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// §3.5/§9 FINISHED-status inventory-adjusted-reflex publish tests for
/// <see cref="InventoryAdjustedReflexPublisher"/> (docs/events/inventory.InternalHallmarkingStatusChanged.md),
/// mirroring <see cref="DeltaTowardsOmsPublisherTests"/>'s shape.
/// </summary>
public class InventoryAdjustedReflexPublisherTests
{
    private sealed class NoOpLogger : ILogger<InventoryAdjustedReflexPublisher>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
        }
    }

    private readonly IServiceBusRelayPublisher relayPublisher = Substitute.For<IServiceBusRelayPublisher>();
    private readonly ICorrelationContext correlationContext = Substitute.For<ICorrelationContext>();
    private readonly InventoryAdjustedReflexPublisher sut;

    public InventoryAdjustedReflexPublisherTests()
    {
        correlationContext.CorrelationId.Returns("corr-1");
        correlationContext.AppId.Returns("app-1");

        var publishOptions = Substitute.For<IOptions<InventoryPublishOptions>>();
        publishOptions.Value.Returns(new InventoryPublishOptions { InventoryAdjustedReflexQueueName = "inventory-adjusted-reflex" });

        sut = new InventoryAdjustedReflexPublisher(
            relayPublisher, publishOptions, correlationContext, new NoOpLogger());
    }

    [Fact(DisplayName = "PublishAsync serializes every field of the completed transit into the request payload")]
    public async Task PublishAsync_Always_PopulatesRequestFieldsFromInputs()
    {
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);
        var adjustmentDate = new DateTime(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc);

        await sut.PublishAsync(
            "OwnOnline", "hallmark-1", adjustmentDate, "WH-1", "Warehouse", "ORG-1",
            "SKU-1", 4, "TH", "925",
            State.AVAILABLE, Status.HALLMARKING,
            "hallmark-1", CancellationToken.None);

        var request = JsonSerializer.Deserialize<InventoryAdjustedReflexPublishRequest>(captured!.Json)!;
        Assert.Equal("OwnOnline", request.Channel);
        Assert.Equal("hallmark-1", request.Id);
        Assert.Equal(adjustmentDate, request.AdjustmentDate);
        Assert.Equal("WH-1", request.Location.Id);
        Assert.Equal("Warehouse", request.Location.Type);
        Assert.Equal("ORG-1", request.Entity);
        Assert.Equal("SKU-1", request.ItemCode);
        Assert.Equal(4, request.Quantity);
        Assert.Equal("TH", request.CountryOfOrigin);
        Assert.Equal("925", request.HallmarkTo);
        Assert.Equal("AVAILABLE", request.ToState.State);
        Assert.Equal("HALLMARKING", request.ToState.Status);
        Assert.Equal("hallmark-1", request.ReferenceId);
    }

    [Fact(DisplayName = "PublishAsync relays onto the configured inventory-adjusted-reflex queue with a deterministic SessionId/MessageId and the expected event type")]
    public async Task PublishAsync_Always_RelaysToConfiguredQueueWithDeterministicIdsAndExpectedType()
    {
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "OwnOnline", "hallmark-1", DateTime.UtcNow, "WH-1", "Warehouse", "ORG-1",
            "SKU-1", 4, "TH", "925",
            State.AVAILABLE, Status.HALLMARKING,
            "hallmark-1", CancellationToken.None);

        Assert.Equal("inventory-adjusted-reflex", captured!.QueueName);
        Assert.Equal("hallmark-1", captured.SessionId);
        Assert.Equal("hallmark-1:Inventory_InternalHallmarkingInventoryAdjusted", captured.MessageId);
        Assert.Equal(["Inventory_InternalHallmarkingInventoryAdjusted"], captured.Types);
        Assert.Equal("corr-1", captured.CorrelationId);
        Assert.Equal("app-1", captured.AppId);
    }

    [Fact(DisplayName = "PublishAsync passes the cancellation token through to the relay publisher")]
    public async Task PublishAsync_Always_PassesCancellationTokenThrough()
    {
        using var cts = new CancellationTokenSource();
        relayPublisher.PublishAsync(Arg.Any<ServiceBusRelayMessage>(), cts.Token)
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "OwnOnline", "hallmark-1", DateTime.UtcNow, "WH-1", "Warehouse", "ORG-1",
            "SKU-1", 4, "TH", "925",
            State.AVAILABLE, Status.HALLMARKING,
            "hallmark-1", cts.Token);

        await relayPublisher.Received(1).PublishAsync(Arg.Any<ServiceBusRelayMessage>(), cts.Token);
    }
}
