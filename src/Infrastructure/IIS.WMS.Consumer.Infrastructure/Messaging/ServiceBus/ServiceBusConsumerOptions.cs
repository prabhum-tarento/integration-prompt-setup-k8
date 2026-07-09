namespace IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;

/// <summary>Bound from the <c>ServiceBus</c> configuration section.</summary>
public sealed class ServiceBusConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "ServiceBus";

    /// <summary>Fully-qualified namespace, e.g. <c>my-namespace.servicebus.windows.net</c> - required for Workload Identity auth (no connection string to derive it from).</summary>
    public string FullyQualifiedNamespace { get; init; } = default!;

    /// <summary>Name of the session-enabled queue the relay publishes to and the consumer processes.</summary>
    public string QueueName { get; init; } = "inventory-events";
}
