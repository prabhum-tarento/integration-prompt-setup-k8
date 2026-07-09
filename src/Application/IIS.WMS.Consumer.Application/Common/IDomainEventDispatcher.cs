using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// Publishes an aggregate's collected domain events after its changes are persisted. Called from
/// the Application layer, never from inside the aggregate itself - publishing before the write
/// commits would let a handler observe a state change that could still roll back
/// (oop-design-patterns.instructions.md - Observer).
/// </summary>
public interface IDomainEventDispatcher
{
    /// <summary>Publishes each event in <paramref name="domainEvents"/>, in order, via the underlying notification pipeline.</summary>
    /// <param name="domainEvents">The aggregate's collected domain events - typically its <c>DomainEvents</c> property, read after a successful save.</param>
    /// <param name="cancellationToken">Token to cancel dispatch.</param>
    Task DispatchAsync(IReadOnlyCollection<IDomainEvent> domainEvents, CancellationToken cancellationToken = default);
}
