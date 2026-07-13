namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// Carries the correlation id for the current request or message across the Kafka → Service Bus →
/// Cosmos DB pipeline (integration-resiliency.instructions.md §4). Registered as a scoped service;
/// set once at the HTTP or message-consumer boundary and never regenerated mid-flow.
/// </summary>
public interface ICorrelationContext
{
    /// <summary>The current request's or message's correlation id, or an empty string before <see cref="Set(string)"/> has been called.</summary>
    string CorrelationId { get; }

    /// <summary>The producing application's id, or an empty string before <see cref="Set(string, string, IReadOnlyList{string})"/> has been called - only populated at the Kafka consumer boundary.</summary>
    string AppId { get; }
    
    string Type { get; }

    /// <summary>The Kafka event type and consumer name for this message, or an empty list before <see cref="Set(string, string, IReadOnlyList{string})"/> has been called - only populated at the Kafka consumer boundary.</summary>
    IReadOnlyList<string> Types { get; }

    /// <summary>Sets the correlation id for the remainder of this scope. Called exactly once, at the HTTP or Service Bus consumer boundary.</summary>
    void Set(string correlationId);

    /// <summary>Sets the correlation id, app id, and event/consumer types for the remainder of this scope. Called exactly once, at the Kafka consumer boundary.</summary>
    /// <param name="correlationId">The message's correlation id, from the Kafka <c>CorrelationId</c> header.</param>
    /// <param name="appId">The producing application's id, from the Kafka <c>AppId</c> header.</param>
    /// <param name="types">The Kafka <c>Type</c> header value and the relaying consumer's name.</param>
    void Set(string correlationId, string appId, IReadOnlyList<string> types);
}
