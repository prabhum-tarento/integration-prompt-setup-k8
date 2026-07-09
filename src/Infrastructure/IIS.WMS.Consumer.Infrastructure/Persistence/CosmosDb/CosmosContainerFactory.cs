using System.Collections.Concurrent;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>
/// Singleton container cache backing every repository's <see cref="CosmosRepository{TDomain,TDocument}"/>.
/// Registered once (see <see cref="CosmosDbServiceCollectionExtensions"/>) and reused for the life of the
/// process, the same lifetime the single <c>Container</c> singleton used to have before container names moved
/// into individual repositories - <see cref="Container"/> instances are cheap client-side handles, so caching
/// here just avoids re-resolving the same name from every repository construction, not a network call.
/// </summary>
public sealed class CosmosContainerFactory(CosmosClient client, IOptions<CosmosDbOptions> options) : ICosmosContainerFactory
{
    private readonly ConcurrentDictionary<string, Container> _containers = new();

    /// <inheritdoc />
    public Container GetContainer(string containerName) =>
        _containers.GetOrAdd(containerName, name => client.GetContainer(options.Value.DatabaseName, name));
}
