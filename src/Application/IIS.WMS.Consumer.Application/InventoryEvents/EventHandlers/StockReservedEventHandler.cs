using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Events;
using MediatR;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.InventoryEvents.EventHandlers;

/// <summary>
/// Sample domain-event handler wired through MediatR (oop-design-patterns.instructions.md -
/// Observer). Reserving stock is business-significant enough to log independently of the
/// triggering HTTP request/message, so it survives even if the caller's own logging changes.
/// </summary>
public sealed class StockReservedEventHandler(ILogger<StockReservedEventHandler> logger)
    : INotificationHandler<DomainEventNotification<StockReserved>>
{
    /// <summary>Logs the reservation. This handler has no side effects beyond logging - it does not touch persistence or publish further events.</summary>
    /// <param name="notification">The wrapped <see cref="StockReserved"/> event.</param>
    /// <param name="cancellationToken">Token to cancel handling.</param>
    public Task Handle(DomainEventNotification<StockReserved> notification, CancellationToken cancellationToken)
    {
        var stockReserved = notification.DomainEvent;

        logger.LogInformation(
            "Stock reserved: {Quantity} unit(s) of {Sku} at {WarehouseId} under reservation {ReservationId}.",
            stockReserved.Quantity, stockReserved.Sku, stockReserved.WarehouseId, stockReserved.ReservationId);

        return Task.CompletedTask;
    }
}
