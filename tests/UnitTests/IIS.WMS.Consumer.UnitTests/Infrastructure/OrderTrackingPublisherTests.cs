using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.OrderTracking.Dtos;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Egress;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// §3.9 order-tracking publish tests for <see cref="OrderTrackingPublisher"/>
/// (docs/events/inventory.InventoryStateChanged.md), with the relay publisher mocked.
/// </summary>
public class OrderTrackingPublisherTests
{
    private readonly IServiceBusRelayPublisher relayPublisher = Substitute.For<IServiceBusRelayPublisher>();
    private readonly ICorrelationContext correlationContext = Substitute.For<ICorrelationContext>();
    private readonly OrderTrackingPublisher sut;

    public OrderTrackingPublisherTests()
    {
        correlationContext.CorrelationId.Returns("corr-1");
        correlationContext.AppId.Returns("app-1");

        var publishOptions = Substitute.For<IOptions<InventoryPublishOptions>>();
        publishOptions.Value.Returns(new InventoryPublishOptions { OrderTrackingQueueName = "order-tracking" });

        sut = new OrderTrackingPublisher(
            relayPublisher, publishOptions, correlationContext, NullLogger<OrderTrackingPublisher>.Instance);
    }

    private static OrderTrackingRelayRequest CreateRequest(string referenceId = "state-1") => new(
        ReferenceId: referenceId,
        Channel: "OwnOnline",
        FulfilmentUnitId: "WH-1",
        FulfilmentUnitType: "Warehouse",
        FunctionName: "InventoryStateChangedHandler",
        OrderId: "REF-1",
        OrderStatus: OrderTrackingStatus.PICKED,
        OrderType: "SALES",
        Lines: [new OrderTrackingRelayLine("SKU-1", "TH", "925", 2)]);

    [Fact(DisplayName = "PublishAsync relays onto the configured order-tracking queue with the expected event type and session/message ids")]
    public async Task PublishAsync_Always_RelaysToConfiguredQueueWithExpectedTypeAndIds()
    {
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(CreateRequest(), CancellationToken.None);

        Assert.Equal("order-tracking", captured!.QueueName);
        Assert.Equal("state-1", captured.SessionId);
        Assert.Equal("state-1:OrderTrackingCommonRequest", captured.MessageId);
        Assert.Equal(["OrderTrackingCommonRequest"], captured.Types);
        Assert.Equal("corr-1", captured.CorrelationId);
        Assert.Equal("app-1", captured.AppId);
    }

    [Fact(DisplayName = "PublishAsync serializes the request fields into the relayed message's Json payload")]
    public async Task PublishAsync_Always_SerializesRequestIntoJsonPayload()
    {
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(CreateRequest(), CancellationToken.None);

        var payload = JsonSerializer.Deserialize<OrderTrackingRelayRequest>(captured!.Json)!;
        Assert.Equal("state-1", payload.ReferenceId);
        Assert.Equal("REF-1", payload.OrderId);
        Assert.Equal(OrderTrackingStatus.PICKED, payload.OrderStatus);
        Assert.Single(payload.Lines);
        Assert.Equal("SKU-1", payload.Lines[0].ItemCode);
    }

    [Fact(DisplayName = "PublishAsync swallows a non-cancellation publish failure rather than propagating it")]
    public async Task PublishAsync_RelayPublisherThrows_SwallowsException()
    {
        relayPublisher.PublishAsync(Arg.Any<ServiceBusRelayMessage>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("simulated relay failure"));

        var exception = await Record.ExceptionAsync(() => sut.PublishAsync(CreateRequest(), CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact(DisplayName = "PublishAsync does not swallow an OperationCanceledException from the relay publisher")]
    public async Task PublishAsync_RelayPublisherThrowsOperationCanceled_PropagatesException()
    {
        relayPublisher.PublishAsync(Arg.Any<ServiceBusRelayMessage>(), Arg.Any<CancellationToken>())
            .Throws(new OperationCanceledException());

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => sut.PublishAsync(CreateRequest(), CancellationToken.None));
    }
}
