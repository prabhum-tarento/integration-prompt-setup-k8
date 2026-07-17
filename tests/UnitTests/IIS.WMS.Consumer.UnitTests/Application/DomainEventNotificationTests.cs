using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Events;
using MediatR;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Wrapper tests for <see cref="DomainEventNotification{TDomainEvent}"/>.</summary>
public class DomainEventNotificationTests
{
    [Fact(DisplayName = "Constructor exposes the wrapped domain event")]
    public void Constructor_DomainEvent_ExposesWrappedEvent()
    {
        var stockReserved = new StockReserved("WH1:SKU1", "WH1", "SKU1", 5, "reservation-1");

        var notification = new DomainEventNotification<StockReserved>(stockReserved);

        Assert.Same(stockReserved, notification.DomainEvent);
    }

    [Fact(DisplayName = "The wrapper implements MediatR's INotification marker")]
    public void Notification_Always_IsMediatRNotification()
    {
        var stockReserved = new StockReserved("WH1:SKU1", "WH1", "SKU1", 5, "reservation-1");

        var notification = new DomainEventNotification<StockReserved>(stockReserved);

        Assert.IsAssignableFrom<INotification>(notification);
    }
}
