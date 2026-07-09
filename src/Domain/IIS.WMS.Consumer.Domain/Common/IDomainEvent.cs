namespace IIS.WMS.Consumer.Domain.Common;

/// <summary>
/// Marker for a business-significant occurrence raised by an aggregate. Kept free of any
/// messaging-library dependency (e.g. MediatR's <c>INotification</c>) so the Domain layer stays
/// free of external package references; the Application layer wraps these for dispatch.
/// </summary>
public interface IDomainEvent
{
    /// <summary>Unique identifier for this specific occurrence of the event - distinct from any aggregate or entity id.</summary>
    Guid EventId { get; }

    /// <summary>UTC timestamp of when the event was raised.</summary>
    DateTime OccurredOnUtc { get; }
}
