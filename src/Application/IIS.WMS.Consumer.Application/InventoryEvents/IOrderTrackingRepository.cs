using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Port for <see cref="OrderTracking"/> reads (cosmos-db.instructions.md §5,
/// docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.3 step 1, §5.4). Read-only - the tracking
/// status write is a downstream consumer's responsibility, not this event's. Implemented by
/// <c>Infrastructure.Persistence.CosmosDb.Repository.OrderTrackingRepository</c>, which routes to a
/// per-fulfilment-code container by parsing <paramref name="category"/>'s first <c>:</c>-delimited
/// segment (per Q3), mirroring <c>ItemStockInventoryRepository</c>.
/// </summary>
public interface IOrderTrackingRepository
{
    /// <summary>Reads a single record by id, or <see langword="null"/> if it doesn't exist.</summary>
    /// <param name="id">Record id.</param>
    /// <param name="category">Cosmos partition key (same value as <paramref name="id"/>).</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    Task<Domain.Aggregates.OrderTracking?> GetAsync(string id, string category, CancellationToken cancellationToken = default);
}
