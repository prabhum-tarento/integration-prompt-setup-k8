namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// Carries the correlation id for the current request or message across the Kafka → Service Bus →
/// Cosmos DB pipeline (integration-resiliency.instructions.md §4). Registered as a scoped service;
/// set once at the HTTP or message-consumer boundary and never regenerated mid-flow.
/// </summary>
public interface ICorrelationContext
{
    /// <summary>The current request's or message's correlation id, or an empty string before <see cref="Set"/> has been called.</summary>
    string CorrelationId { get; }

    /// <summary>Sets the correlation id for the remainder of this scope. Called exactly once, at the HTTP or message-consumer boundary.</summary>
    void Set(string correlationId);
}
