using Microsoft.Azure.Cosmos;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>
/// Resolves a Cosmos <see cref="Container"/> by name against the shared database (cosmos-db.instructions.md
/// §2 - <c>Container</c> is a singleton, resolved once from <c>CosmosClient</c>). Each repository supplies its
/// own container name at construction (§1) instead of every repository sharing one configured
/// <c>CosmosDb:ContainerName</c> setting.
/// </summary>
public interface ICosmosContainerFactory
{
    /// <summary>Returns the container for <paramref name="containerName"/>, resolving and caching it on first use.</summary>
    Container GetContainer(string containerName);
}
