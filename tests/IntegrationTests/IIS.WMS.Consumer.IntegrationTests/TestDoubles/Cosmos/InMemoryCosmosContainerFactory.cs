using System.Collections.Concurrent;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using Microsoft.Azure.Cosmos;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles.Cosmos;

/// <summary>
/// In-memory <see cref="ICosmosContainerFactory"/> - registers in place of the real
/// <c>CosmosContainerFactory</c> (both the default and the <c>"bulk"</c>-keyed registration in
/// <c>CosmosDbServiceCollectionExtensions</c>) so every repository built on
/// <c>CosmosRepository{TDomain,TDocument}</c> runs against <see cref="InMemoryCosmosContainer"/>
/// without any repository-level code change (integration-resiliency.instructions.md §9,
/// cosmos-db.instructions.md §13).
/// </summary>
public sealed class InMemoryCosmosContainerFactory : ICosmosContainerFactory
{
    private readonly ConcurrentDictionary<string, InMemoryCosmosContainer> containers = new();

    public Container GetContainer(string containerName) =>
        containers.GetOrAdd(containerName, name => new InMemoryCosmosContainer(name));

    /// <summary>Returns the underlying fake for a container name, if it has been resolved yet - lets a test assert on stored items directly.</summary>
    public InMemoryCosmosContainer? GetInMemoryContainer(string containerName) =>
        containers.TryGetValue(containerName, out var container) ? container : null;

    /// <summary>Clears every container this factory has resolved so far - useful between tests sharing one factory instance.</summary>
    public void ClearAll()
    {
        foreach (var container in containers.Values)
        {
            container.Clear();
        }
    }
}
