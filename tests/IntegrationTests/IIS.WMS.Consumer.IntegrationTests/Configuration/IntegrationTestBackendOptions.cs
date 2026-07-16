namespace IIS.WMS.Consumer.IntegrationTests.Configuration;

/// <summary>
/// Per-dependency backend selection for integration tests (integration-resiliency.instructions.md §9) -
/// bound from the <c>IntegrationTestBackends</c> configuration section, which layers (lowest to
/// highest precedence) an in-process-only default, <c>appsettings.IntegrationTests.json</c>, and
/// environment variables - see <c>InventoryEventPipelineTests.BuildConfiguration</c>. Kafka is not a
/// toggle here: it is always a real (Testcontainers) broker regardless of these three settings.
/// </summary>
public sealed class IntegrationTestBackendOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "IntegrationTestBackends";

    /// <summary><see cref="BackendMode.Fake"/> (default): <c>InMemoryCosmosContainer</c>. <see cref="BackendMode.ConnectionString"/>: the real <c>CosmosDb</c> section - the <c>InventoryEvents</c> container must already exist (this repo never auto-provisions, cosmos-db.instructions.md §2).</summary>
    public BackendMode Cosmos { get; init; } = BackendMode.Fake;

    /// <summary><see cref="BackendMode.Fake"/> (default): <c>VirtualServiceBusClient</c>, driven directly through <c>HandleMessageAsync</c>. <see cref="BackendMode.ConnectionString"/>: the real <c>ServiceBus</c> section - the queue must already exist, session-enabled, matching <c>ServiceBusConsumerOptions.QueueName</c>.</summary>
    public BackendMode ServiceBus { get; init; } = BackendMode.Fake;

    /// <summary><see cref="BackendMode.Fake"/> (default): <c>InMemoryFileStore</c>. <see cref="BackendMode.ConnectionString"/>: the real <c>BlobStorage</c> section (e.g. Azurite or a real Storage account).</summary>
    public BackendMode BlobStorage { get; init; } = BackendMode.Fake;
}
