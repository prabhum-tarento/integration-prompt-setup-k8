using System.Text.Json;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// §3.6 B2B adjusted/moved publish tests for <see cref="InventoryAdjustedOrMovedPublisher"/>
/// (docs/InventoryStateChangedFullQueueTrigger.md), with the relay publisher mocked.
/// </summary>
public class InventoryAdjustedOrMovedPublisherTests
{
    private readonly IServiceBusRelayPublisher relayPublisher = Substitute.For<IServiceBusRelayPublisher>();
    private readonly ICorrelationContext correlationContext = Substitute.For<ICorrelationContext>();
    private readonly InventoryAdjustedOrMovedPublisher sut;

    public InventoryAdjustedOrMovedPublisherTests()
    {
        correlationContext.Types.Returns(new List<string>());
        var publishOptions = Substitute.For<IOptions<InventoryPublishOptions>>();
        publishOptions.Value.Returns(new InventoryPublishOptions { SapAdjustedOrMovedQueueName = "sap-queue" });

        sut = new InventoryAdjustedOrMovedPublisher(
            relayPublisher, publishOptions, correlationContext,
            NullLogger<InventoryAdjustedOrMovedPublisher>.Instance);
    }

    private static InventoryAdjustedOrMovedLine CreateLine(int qty = 2) => new()
    {
        ItemCode = "SKU1", Qty = qty, CountryOfOrigin = "TH", Hallmarking = "925",
    };

    [Fact(DisplayName = "PublishAsync skips the publish (SAE-2798) when FromState equals ToState, neither is Available, and this isn't a redelivery")]
    public async Task PublishAsync_SameNonAvailableStateNotRedelivery_SkipsPublish()
    {
        await sut.PublishAsync(
            "own-online", "state-1", DateTime.UtcNow, "WH1", "Warehouse", "ORG-1",
            State.BLOCKED, Status.HELD, State.BLOCKED, Status.HELD,
            "REF-1", [CreateLine()], CancellationToken.None);

        await relayPublisher.DidNotReceiveWithAnyArgs().PublishAsync(default!, default);
    }

    [Fact(DisplayName = "PublishAsync still publishes when FromState equals ToState and neither is Available but the correlation context marks this a B2B_INVENTORY_ADJUSTED redelivery")]
    public async Task PublishAsync_SameNonAvailableStateIsRedelivery_Publishes()
    {
        correlationContext.Types.Returns(new List<string> { KafkaEvents.InventoryAdjustedEventType });

        await sut.PublishAsync(
            "own-online", "state-1", DateTime.UtcNow, "WH1", "Warehouse", "ORG-1",
            State.BLOCKED, Status.HELD, State.BLOCKED, Status.HELD,
            "REF-1", [CreateLine()], CancellationToken.None);

        await relayPublisher.Received(1).PublishAsync(Arg.Any<ServiceBusRelayMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "PublishAsync publishes when FromState differs from ToState even without a redelivery marker")]
    public async Task PublishAsync_DifferingStates_Publishes()
    {
        await sut.PublishAsync(
            "own-online", "state-1", DateTime.UtcNow, "WH1", "Warehouse", "ORG-1",
            State.AVAILABLE, Status.PICKABLE, State.BLOCKED, Status.HELD,
            "REF-1", [CreateLine()], CancellationToken.None);

        await relayPublisher.Received(1).PublishAsync(Arg.Any<ServiceBusRelayMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "PublishAsync forces the outbound status to Unknown (SAE-3032) for whichever side isn't Available, without mutating the caller's values")]
    public async Task PublishAsync_NonAvailableSide_ForcesOutboundStatusToUnknown()
    {
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "own-online", "state-1", DateTime.UtcNow, "WH1", "Warehouse", "ORG-1",
            State.AVAILABLE, Status.PICKABLE, State.BLOCKED, Status.HELD,
            "REF-1", [CreateLine()], CancellationToken.None);

        var request = JsonSerializer.Deserialize<InventoryAdjustedOrMovedPublishRequest>(captured!.Json)!;
        Assert.Equal(Status.PICKABLE.ToString(), request.FromState.Status);
        Assert.Equal(Status.UNKNOWN.ToString(), request.ToState.Status);
    }

    [Fact(DisplayName = "PublishAsync normalizes a negative line quantity to its absolute value")]
    public async Task PublishAsync_NegativeLineQty_NormalizesToAbsoluteValue()
    {
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "own-online", "state-1", DateTime.UtcNow, "WH1", "Warehouse", "ORG-1",
            State.AVAILABLE, Status.PICKABLE, State.BLOCKED, Status.HELD,
            "REF-1", [CreateLine(-5)], CancellationToken.None);

        var request = JsonSerializer.Deserialize<InventoryAdjustedOrMovedPublishRequest>(captured!.Json)!;
        Assert.Equal(5, request.Lines[0].Qty);
    }

    [Fact(DisplayName = "PublishAsync generates a new ReferenceId when the caller's ReferenceId is blank")]
    public async Task PublishAsync_BlankReferenceId_GeneratesNewGuid()
    {
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "own-online", "state-1", DateTime.UtcNow, "WH1", "Warehouse", "ORG-1",
            State.AVAILABLE, Status.PICKABLE, State.BLOCKED, Status.HELD,
            referenceId: "   ", lines: [CreateLine()], cancellationToken: CancellationToken.None);

        var request = JsonSerializer.Deserialize<InventoryAdjustedOrMovedPublishRequest>(captured!.Json)!;
        Assert.True(Guid.TryParse(request.ReferenceId, out _));
    }

    [Fact(DisplayName = "PublishAsync relays onto the configured SAP adjusted/moved queue with the expected event type")]
    public async Task PublishAsync_Always_RelaysToConfiguredQueueWithExpectedType()
    {
        ServiceBusRelayMessage? captured = null;
        relayPublisher.PublishAsync(Arg.Do<ServiceBusRelayMessage>(m => captured = m), Arg.Any<CancellationToken>())
            .Returns((ServiceBusRelayPublishResult?)null!);

        await sut.PublishAsync(
            "own-online", "state-1", DateTime.UtcNow, "WH1", "Warehouse", "ORG-1",
            State.AVAILABLE, Status.PICKABLE, State.BLOCKED, Status.HELD,
            "REF-1", [CreateLine()], CancellationToken.None);

        Assert.Equal("sap-queue", captured!.QueueName);
        Assert.Equal(["Inventory_B2BInventoryAdjustedOrMoved"], captured.Types);
    }
}
