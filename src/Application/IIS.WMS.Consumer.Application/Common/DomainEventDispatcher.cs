using IIS.WMS.Consumer.Domain.Common;
using MediatR;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.Common;

/// <inheritdoc cref="IDomainEventDispatcher"/>
public sealed class DomainEventDispatcher(IMediator mediator, ILogger<DomainEventDispatcher> logger) : IDomainEventDispatcher
{
    /// <inheritdoc />
    public async Task DispatchAsync(IReadOnlyCollection<IDomainEvent> domainEvents, CancellationToken cancellationToken = default)
    {
        if (domainEvents.Count == 0)
        {
            logger.LogDebug("DispatchAsync called with no domain events - nothing to publish.");

            return;
        }

        logger.LogDebug("Dispatching {DomainEventCount} domain event(s).", domainEvents.Count);

        foreach (var domainEvent in domainEvents)
        {
            // Reflection is required here because the event's concrete type is only known at
            // runtime - the wrapper generic argument can't be inferred from the IDomainEvent
            // interface reference alone.
            var notificationType = typeof(DomainEventNotification<>).MakeGenericType(domainEvent.GetType());
            var notification = (INotification)Activator.CreateInstance(notificationType, domainEvent)!;

            await mediator.Publish(notification, cancellationToken);

            logger.LogInformation(
                "Published domain event {DomainEventType} ({EventId}).", domainEvent.GetType().Name, domainEvent.EventId);
        }
    }
}
