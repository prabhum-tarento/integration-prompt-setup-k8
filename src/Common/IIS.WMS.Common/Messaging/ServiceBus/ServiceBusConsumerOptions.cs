using Azure.Messaging.ServiceBus;

namespace IIS.WMS.Common.Messaging.ServiceBus;

/// <summary>Bound from the <c>ServiceBus</c> configuration section.</summary>
public sealed class ServiceBusConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "ServiceBus";

    /// <summary>Fully-qualified namespace, e.g. <c>my-namespace.servicebus.windows.net</c> - required for Workload Identity auth (no connection string to derive it from).</summary>
    public string ConnectionString { get; init; } = default!;

    /// <summary>Name of the session-enabled queue the relay publishes to and the consumer processes.</summary>
    public string QueueName { get; init; } = "inventory-events";

    /// <summary>
    /// AMQP transport the shared <see cref="ServiceBusClient"/> (registered in
    /// <see cref="ServiceBusServiceCollectionExtensions"/>) uses to reach the namespace. <see cref="ServiceBusTransportType.AmqpTcp"/>
    /// (the SDK default) is correct on AKS unless outbound port 5671 is blocked by network policy, in
    /// which case <see cref="ServiceBusTransportType.AmqpWebSockets"/> tunnels AMQP over port 443.
    /// </summary>
    public ServiceBusTransportType TransportType { get; init; } = ServiceBusTransportType.AmqpTcp;

    /// <summary>
    /// Retry policy the shared <see cref="ServiceBusClient"/> applies to every send/receive before an
    /// exception reaches this repo's own Polly <c>service-bus-publish</c> pipeline
    /// (integration-resiliency.instructions.md §3) - bound directly from configuration since every
    /// property here (<see cref="ServiceBusRetryOptions.Mode"/>, <see cref="ServiceBusRetryOptions.MaxRetries"/>,
    /// <see cref="ServiceBusRetryOptions.Delay"/>, <see cref="ServiceBusRetryOptions.MaxDelay"/>,
    /// <see cref="ServiceBusRetryOptions.TryTimeout"/>) is already a mutable, bindable SDK type - left
    /// at its SDK defaults (<see cref="ServiceBusRetryMode.Exponential"/>, 3 retries) unless overridden.
    /// </summary>
    public ServiceBusRetryOptions Retry { get; init; } = new();
}
