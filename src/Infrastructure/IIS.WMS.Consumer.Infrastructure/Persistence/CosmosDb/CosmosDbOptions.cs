namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>Bound from the <c>CosmosDb</c> configuration section (cosmos-db.instructions.md §1). No account-key entry here - production authenticates via <see cref="Azure.Identity.DefaultAzureCredential"/>.</summary>
public sealed class CosmosDbOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "CosmosDb";

    /// <summary>Cosmos account endpoint URI.</summary>
    public string AccountEndpoint { get; init; } = default!;

    /// <summary>Name of the Cosmos database containing every repository's container.</summary>
    public string DatabaseName { get; init; } = default!;

    /// <summary>
    /// Partition key path configured on the containers - must match each container's actual provisioned path.
    /// Container names themselves are declared per-repository, not here - see
    /// <see cref="CosmosContainerFactory"/> and cosmos-db.instructions.md §1.
    /// </summary>
    public string PartitionKeyPath { get; init; } = "/category";

    /// <summary>Local development only - the Cosmos DB Emulator's well-known fixed key, read from user-secrets, never appsettings.json.</summary>
    public string? EmulatorKey { get; init; }
}
