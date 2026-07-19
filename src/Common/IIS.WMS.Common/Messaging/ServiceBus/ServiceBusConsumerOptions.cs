using Azure.Messaging.ServiceBus;

namespace IIS.WMS.Common.Messaging.ServiceBus;

/// <summary>Bound from the <c>ServiceBus</c> configuration section.</summary>
/// <remarks>
/// <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentSessions"/> and
/// <see cref="ServiceBusConsumerOptionsBase.MaxConcurrentCallsPerSession"/> are resolved
/// queue-level-first, ServiceBus-level-fallback: a queue-level consumer (e.g.
/// <c>ServiceBus:InventoryStateChanged</c>) that leaves one of these unset inherits it from this
/// (top-level <c>ServiceBus</c> section) value instead of having to repeat it, mirroring Kafka's own
/// event-level/Kafka-level fallback (<c>Kafka.ConsumerOptions.ApplyKafkaLevelDefaults</c>).
/// That's why these are mutable and nullable rather than <see langword="init"/>-only like the rest of
/// this type - the fallback has to fill them in after configuration binding runs, before any consumer
/// reads them. This top-level <c>ServiceBus</c> section itself bottoms out at hardcoded defaults (see
/// <c>MessagingServiceCollectionExtensions.AddServiceBusConsumers</c>'s <c>PostConfigure</c>), so by
/// the time any consumer runs, both are guaranteed non-null - callers still read them defensively
/// (e.g. <c>?? 8</c>) rather than asserting that, since an options POCO built directly in a unit test
/// (bypassing the DI <c>PostConfigure</c> pipeline) wouldn't get that guarantee.
/// </remarks>
public sealed class ServiceBusConsumerOptions : ServiceBusConsumerOptionsBase
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

    /// <summary>
    /// Allow-list of Service Bus consumer names to start - the Service Bus-side mirror of
    /// <c>Kafka:KafkaEventFunctions</c> (<c>Infrastructure.Messaging.Kafka.KafkaConsumerOptions.KafkaEventFunctions</c>),
    /// same "run only the named ones" semantics, just gating the two Service Bus → Cosmos DB
    /// consumers instead of the three Kafka → Service Bus ones. Names match
    /// <c>Infrastructure.Messaging.MessagingServiceCollectionExtensions.InventoryEventsServiceBusKey</c>
    /// ("InventoryEvents") or <c>...BulkInventoryImportServiceBusKey</c> ("BulkInventoryImport").
    /// This gates whether a consumer's hosted service, keyed <c>ServiceBusHealthState</c>, and queue
    /// health check are registered at all - independent of any per-consumer enable/disable flag.
    /// <see langword="null"/> or empty means "no filter - every consumer starts."
    /// </summary>
    public string[]? ServiceBusEventFunctions { get; init; }
}
