namespace IIS.WMS.Consumer.Domain.Common;

/// <summary>
/// Base type for consistency-boundary entities. Collects domain events raised by the aggregate's
/// own methods; the Application layer publishes them via <c>IMediator.Publish</c> after the
/// aggregate's changes are persisted (Observer, dispatched through MediatR notifications per
/// oop-design-patterns.instructions.md) - the aggregate itself never invokes a handler directly.
/// </summary>
public abstract class AggregateRoot
{
    private readonly List<IDomainEvent> domainEvents = [];

    /// <summary>The aggregate's unique, deterministic identity (see the derived aggregate's factory method for how it's derived).</summary>
    public string Id { get; protected init; } = default!;

    /// <summary>Domain events raised since the aggregate was created or last cleared, in the order they were raised.</summary>
    public IReadOnlyCollection<IDomainEvent> DomainEvents => domainEvents.AsReadOnly();

    /// <summary>Clears the collected domain events - called by the Application layer once it has dispatched them, so they aren't re-published on the next save.</summary>
    public void ClearDomainEvents()
    {
        domainEvents.Clear();
    }

    /// <summary>Records a domain event for later dispatch. Called only from within a business method, after the aggregate's state has already changed.</summary>
    protected void RaiseDomainEvent(IDomainEvent domainEvent)
    {
        domainEvents.Add(domainEvent);
    }
}
