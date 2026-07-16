namespace IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;

/// <summary>The three ways a Service Bus consumer's message-handling core can conclude - see <see cref="ServiceBusMessageOutcome"/>.</summary>
public enum ServiceBusMessageOutcomeKind
{
    /// <summary>Applied successfully - settle by completing the message.</summary>
    Completed,

    /// <summary>A transient/processing failure (including an exhausted concurrency retry) - settle by abandoning, for Service Bus to redeliver.</summary>
    Abandoned,

    /// <summary>Poison or otherwise unprocessable - settle by dead-lettering with <see cref="Reason"/>/<see cref="Description"/>.</summary>
    DeadLettered,
}

/// <summary>
/// Result of handling one received message, decoupled from the Service Bus SDK's settlement API
/// (<c>CompleteMessageAsync</c>/<c>AbandonMessageAsync</c>/<c>DeadLetterMessageAsync</c> on
/// <c>ProcessSessionMessageEventArgs</c>/<c>ProcessMessageEventArgs</c>). Both
/// <see cref="ServiceBusConsumerHostedService"/> and <see cref="BulkImportServiceBusConsumerHostedService"/>
/// split their real event handler into a thin adapter (still wired to the real SDK event, settles the
/// message per this outcome) and an <c>internal HandleMessageAsync(ServiceBusReceivedMessage,
/// CancellationToken)</c> core that returns this type instead of calling settlement methods directly -
/// this is what lets an integration test exercise that core logic by building a message via
/// <c>ServiceBusModelFactory</c> and asserting the returned outcome, without ever needing a working
/// <c>ServiceBusSessionProcessor</c>/<c>ServiceBusProcessor</c> (integration-resiliency.instructions.md §9;
/// subscribing to either type's <c>ProcessMessageAsync</c> event on a processor not built through a real
/// connected <c>ServiceBusClient</c> throws on the subscription itself, a hard SDK limitation, not a
/// design choice made here).
/// </summary>
public sealed record ServiceBusMessageOutcome(ServiceBusMessageOutcomeKind Kind, string? Reason = null, string? Description = null)
{
    public static readonly ServiceBusMessageOutcome Completed = new(ServiceBusMessageOutcomeKind.Completed);

    public static readonly ServiceBusMessageOutcome Abandoned = new(ServiceBusMessageOutcomeKind.Abandoned);

    public static ServiceBusMessageOutcome DeadLettered(string reason, string? description = null) =>
        new(ServiceBusMessageOutcomeKind.DeadLettered, reason, description);
}
