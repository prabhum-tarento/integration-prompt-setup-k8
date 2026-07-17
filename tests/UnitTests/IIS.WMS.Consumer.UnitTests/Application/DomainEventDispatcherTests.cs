using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Domain.Events;
using MediatR;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Publish fan-out tests for <see cref="DomainEventDispatcher"/>, with MediatR's <see cref="IMediator"/> mocked.</summary>
public class DomainEventDispatcherTests
{
    private readonly IMediator mediator = Substitute.For<IMediator>();
    private readonly DomainEventDispatcher sut;

    public DomainEventDispatcherTests()
    {
        sut = new DomainEventDispatcher(mediator, Substitute.For<ILogger<DomainEventDispatcher>>());
    }

    [Fact(DisplayName = "DispatchAsync does not publish anything when given zero domain events")]
    public async Task DispatchAsync_NoEvents_DoesNotPublish()
    {
        await sut.DispatchAsync([]);

        await mediator.DidNotReceive().Publish(Arg.Any<INotification>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "DispatchAsync wraps and publishes a single domain event via MediatR")]
    public async Task DispatchAsync_SingleEvent_PublishesWrappedNotification()
    {
        var stockReserved = new StockReserved("WH1:SKU1", "WH1", "SKU1", 5, "reservation-1");

        await sut.DispatchAsync([stockReserved]);

        await mediator.Received(1).Publish(
            Arg.Is<INotification>(n => IsWrapping(n, stockReserved)),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "DispatchAsync publishes every event in the collection, each wrapped in its own notification type")]
    public async Task DispatchAsync_MultipleEvents_PublishesEachWrappedInItsOwnNotificationType()
    {
        var stockReserved = new StockReserved("WH1:SKU1", "WH1", "SKU1", 5, "reservation-1");
        var stockAllocated = new StockAllocated("WH1:SKU1", "WH1", "SKU1", 5, "reservation-1");

        await sut.DispatchAsync([stockReserved, stockAllocated]);

        await mediator.Received(1).Publish(
            Arg.Is<INotification>(n => IsWrapping(n, stockReserved)),
            Arg.Any<CancellationToken>());
        await mediator.Received(1).Publish(
            Arg.Is<INotification>(n => IsWrapping(n, stockAllocated)),
            Arg.Any<CancellationToken>());
    }

    private static bool IsWrapping<TEvent>(INotification notification, TEvent domainEvent)
        where TEvent : IDomainEvent
        => notification is DomainEventNotification<TEvent> wrapped && Equals(wrapped.DomainEvent, domainEvent);

    [Fact(DisplayName = "DispatchAsync passes the provided cancellation token through to each publish call")]
    public async Task DispatchAsync_ProvidedCancellationToken_PassesTokenToPublish()
    {
        using var cts = new CancellationTokenSource();
        var stockReserved = new StockReserved("WH1:SKU1", "WH1", "SKU1", 5, "reservation-1");

        await sut.DispatchAsync([stockReserved], cts.Token);

        await mediator.Received(1).Publish(Arg.Any<INotification>(), cts.Token);
    }
}
