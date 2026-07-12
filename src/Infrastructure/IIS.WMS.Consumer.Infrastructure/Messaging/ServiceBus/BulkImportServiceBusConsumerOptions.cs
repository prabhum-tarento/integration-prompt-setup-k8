namespace IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;

/// <summary>
/// Bound from the <c>ServiceBus:BulkInventoryImport</c> configuration section - settings for the
/// <b>non-session</b> queue the bulk-import Kafka consumer relays onto
/// (integration-resiliency.instructions.md §1/§2). Deliberately a separate options type from
/// <see cref="ServiceBusConsumerOptions"/>, not an extra field on it, since this queue has no session
/// requirement and is expected to need its own throughput tuning (<see cref="MaxConcurrentCalls"/>)
/// independent of the session-enabled queue's <c>MaxConcurrentSessions</c>/<c>MaxConcurrentCallsPerSession</c>.
/// </summary>
public sealed class BulkImportServiceBusConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "ServiceBus:BulkInventoryImport";

    /// <summary>Name of the non-session queue the bulk-import relay publishes to and this consumer processes.</summary>
    public string QueueName { get; init; } = "inventory-bulk-import";

    /// <summary>
    /// SDK-managed concurrent message dispatch (the non-session equivalent of <c>MaxConcurrentSessions</c>) -
    /// this is the whole worker pool for this consumer; no in-process <c>Channel</c> needed on top of it
    /// (see integration-resiliency.instructions.md §6's Channel-vs-SDK-concurrency discussion for why).
    /// Tune alongside <see cref="Persistence.CosmosDb.CosmosDbServiceCollectionExtensions.BulkCosmosClientKey"/>'s
    /// <c>AllowBulkExecution</c> Cosmos client and its provisioned RU/s.
    /// </summary>
    public int MaxConcurrentCalls { get; init; } = 32;
}
