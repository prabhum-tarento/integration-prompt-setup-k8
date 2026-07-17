namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// Relays messages onto Azure Service Bus following the shared <c>ServiceBusRelayEnvelope</c> format -
/// the one place every caller (Kafka relay consumers, Service Bus-side handlers, application services)
/// builds that envelope, claim-check offloads an oversized payload to blob storage, and sends through
/// the Service Bus resilience pipeline, instead of each duplicating that logic. A publish failure -
/// even after the resilience pipeline's own retries are exhausted - propagates as an exception; this
/// interface has no failure policy of its own, so each caller decides whether that means dead-lettering
/// and continuing (as the Kafka consumers do) or failing the caller's own operation.
/// </summary>
public interface IServiceBusRelayPublisher
{
    /// <summary>Relays one message.</summary>
    /// <param name="message">The payload and routing/correlation fields to relay.</param>
    /// <param name="cancellationToken">Token to cancel the publish.</param>
    /// <returns>Claim-check/publish diagnostics for the relayed message.</returns>
    Task<ServiceBusRelayPublishResult> PublishAsync(ServiceBusRelayMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Relays many messages, grouped by <see cref="ServiceBusRelayMessage.QueueName"/> and packed into
    /// as few Service Bus batches per queue as fit within the SDK's own per-batch size limit - see
    /// Microsoft's "send batches of messages" guidance. Messages for different queues are otherwise
    /// unrelated; one queue's batch failing does not stop another queue's messages from being sent.
    /// </summary>
    /// <param name="messages">The messages to relay - may span more than one queue.</param>
    /// <param name="cancellationToken">Token to cancel the publish.</param>
    /// <returns>Claim-check/publish diagnostics for each relayed message, in the same order as <paramref name="messages"/>.</returns>
    Task<IReadOnlyList<ServiceBusRelayPublishResult>> PublishBatchAsync(
        IReadOnlyCollection<ServiceBusRelayMessage> messages, CancellationToken cancellationToken = default);
}
