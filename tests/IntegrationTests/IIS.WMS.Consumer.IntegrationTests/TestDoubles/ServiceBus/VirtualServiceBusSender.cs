using Azure.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles.ServiceBus;

/// <summary>
/// In-process <see cref="ServiceBusSender"/> - built via the SDK's own parameterless constructor
/// (provided specifically for mocking/subclassing, per Azure SDK guidelines), overriding
/// <see cref="SendMessageAsync"/>/<see cref="SendMessagesAsync(System.Collections.Generic.IEnumerable{ServiceBusMessage},System.Threading.CancellationToken)"/>
/// to forward into <see cref="VirtualServiceBusBroker.DispatchAsync"/> instead of a real network call
/// (integration-resiliency.instructions.md §9). <c>ConsumerHostedService</c>'s (the Kafka relay)
/// existing sender-cache logic keeps working unmodified against whatever this returns - no production
/// code change needed for the publish side.
/// </summary>
public sealed class VirtualServiceBusSender(string queueName, VirtualServiceBusBroker broker) : ServiceBusSender
{
    public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default) =>
        broker.DispatchAsync(queueName, message, cancellationToken);

    public override async Task SendMessagesAsync(IEnumerable<ServiceBusMessage> messages, CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
        {
            await broker.DispatchAsync(queueName, message, cancellationToken);
        }
    }

    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
