namespace IIS.WMS.Consumer.IntegrationTests.Configuration;

/// <summary>
/// Which backend an integration test talks to for one dependency (Cosmos DB, Service Bus, Blob
/// Storage) - see <see cref="IntegrationTestBackendOptions"/>.
/// </summary>
public enum BackendMode
{
    /// <summary>The in-process fake (<c>InMemoryCosmosContainer</c>/<c>VirtualServiceBusClient</c>/<c>InMemoryFileStore</c>) - the default, needs no external dependency running.</summary>
    Fake,

    /// <summary>A real backend, reached through this repo's own production connection-string/endpoint configuration (<c>CosmosDb</c>, <c>ServiceBus</c>, <c>BlobStorage</c> sections) - e.g. the Cosmos DB Emulator, a real Service Bus namespace, or Azurite.</summary>
    ConnectionString,
}
