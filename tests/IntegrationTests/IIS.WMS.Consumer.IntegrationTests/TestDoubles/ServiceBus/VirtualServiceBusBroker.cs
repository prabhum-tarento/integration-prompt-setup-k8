using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles.ServiceBus;

/// <summary>
/// Shared in-process router behind the Virtual Service Bus (integration-resiliency.instructions.md §9) -
/// the counterpart of the sibling repo's <c>VirtualServiceBusQueue</c>, adapted to this repo's
/// direct-SDK architecture (no <c>IServiceBusQueueService</c>/Azure Functions trigger abstraction to hook
/// into here). A queue name maps to a handler that a test registers directly against the extracted
/// <c>HandleMessageAsync</c> core method on <c>ServiceBusConsumerHostedService</c>/
/// <c>BulkImportServiceBusConsumerHostedService</c> - <see cref="VirtualServiceBusSender"/> calls
/// <see cref="DispatchAsync"/> instead of publishing to a real broker, converting the outbound
/// <see cref="ServiceBusMessage"/> into a <see cref="ServiceBusReceivedMessage"/> via
/// <see cref="ServiceBusModelFactory"/> - a real, public SDK factory method meant exactly for this.
/// </summary>
public sealed class VirtualServiceBusBroker
{
    private readonly ConcurrentDictionary<string, Func<ServiceBusReceivedMessage, CancellationToken, Task>> routes =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Registers <paramref name="handler"/> as the consumer for <paramref name="queueName"/> - a later <see cref="DispatchAsync"/> for the same name invokes it directly, in-process.</summary>
    public void RegisterQueue(string queueName, Func<ServiceBusReceivedMessage, CancellationToken, Task> handler) =>
        routes[queueName] = handler;

    /// <summary>Converts <paramref name="message"/> into a <see cref="ServiceBusReceivedMessage"/> and hands it to whatever's registered for <paramref name="queueName"/> - a no-op if nothing is (mirrors a real broker with no active consumer on that queue).</summary>
    public async Task DispatchAsync(string queueName, ServiceBusMessage message, CancellationToken cancellationToken)
    {
        if (!routes.TryGetValue(queueName, out var handler))
        {
            return;
        }

        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: message.Body,
            messageId: message.MessageId,
            sessionId: message.SessionId,
            correlationId: message.CorrelationId,
            properties: message.ApplicationProperties,
            enqueuedTime: DateTimeOffset.UtcNow,
            deliveryCount: 1);

        await handler(received, cancellationToken);
    }
}
