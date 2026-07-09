using IIS.WMS.Consumer.Domain.Common;
using MediatR;

namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// Wraps a Domain event as a MediatR notification. Domain events implement the dependency-free
/// <see cref="IDomainEvent"/> marker (Domain has no external package references); this wrapper is
/// what lets <see cref="DomainEventDispatcher"/> publish them via <c>IMediator.Publish</c> without
/// the Domain layer itself referencing MediatR (oop-design-patterns.instructions.md - Observer).
/// </summary>
public sealed class DomainEventNotification<TDomainEvent>(TDomainEvent domainEvent) : INotification
    where TDomainEvent : IDomainEvent
{
    /// <summary>The wrapped Domain event.</summary>
    public TDomainEvent DomainEvent { get; } = domainEvent;
}
