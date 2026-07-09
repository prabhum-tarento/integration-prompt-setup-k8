namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>
/// Minimum shape a Cosmos persistence document must expose so <see cref="CosmosRepository{TDomain,TDocument}"/>
/// can log and dedupe generically without knowing the concrete document type (cosmos-db.instructions.md §3).
/// </summary>
public interface ICosmosDocument
{
    /// <summary>Deterministic item id.</summary>
    string Id { get; }

    /// <summary>Cosmos partition key value.</summary>
    string PartitionKey { get; }

    /// <summary>Cosmos's system-managed optimistic-concurrency token.</summary>
    string? ETag { get; }
}
