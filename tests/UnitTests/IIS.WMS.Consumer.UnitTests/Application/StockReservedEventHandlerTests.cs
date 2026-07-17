using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents.EventHandlers;
using IIS.WMS.Consumer.Domain.Events;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>
/// Tests for the <see cref="StockReservedEventHandler"/> MediatR notification handler. The handler's
/// only observable effect is a log statement - per this codebase's convention (see the other
/// Infrastructure test fixtures), <see cref="ILogger{TCategoryName}"/> is passed as an inert
/// substitute rather than asserted against: NSubstitute's generic-method matching for
/// <c>ILogger.Log&lt;TState&gt;</c> requires the exact closed <c>TState</c> the logging extension
/// method infers at its call site (an internal, non-public type), so a same-signature
/// <c>Received().Log(...)</c> assertion would never match and would make the test brittle without
/// adding real coverage - completing without throwing already exercises the log statement's line.
/// </summary>
public class StockReservedEventHandlerTests
{
    private readonly StockReservedEventHandler sut = new(Substitute.For<ILogger<StockReservedEventHandler>>());

    [Fact(DisplayName = "Handle completes without touching persistence or publishing further events")]
    public async Task Handle_StockReservedNotification_CompletesSuccessfully()
    {
        var stockReserved = new StockReserved("WH1:SKU1", "WH1", "SKU1", 5, "reservation-1");
        var notification = new DomainEventNotification<StockReserved>(stockReserved);

        await sut.Handle(notification, CancellationToken.None);
    }

    [Fact(DisplayName = "Handle returns an already-completed task")]
    public void Handle_StockReservedNotification_ReturnsCompletedTask()
    {
        var stockReserved = new StockReserved("WH1:SKU1", "WH1", "SKU1", 5, "reservation-1");
        var notification = new DomainEventNotification<StockReserved>(stockReserved);

        var task = sut.Handle(notification, CancellationToken.None);

        Assert.True(task.IsCompletedSuccessfully);
    }
}
