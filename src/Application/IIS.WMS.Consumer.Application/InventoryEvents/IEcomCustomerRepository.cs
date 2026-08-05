using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Port for <see cref="EcomCustomer"/> reference-data reads (cosmos-db.instructions.md §5,
/// docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.3 step 1). Backed by the shared
/// <c>MasterData</c> container (per Q2) - consumers should depend on this interface, which is
/// implemented by an <see cref="IMemoryCache"/>-backed decorator wrapping the raw Cosmos repository
/// so this reference data isn't re-fetched from Cosmos on every message.
/// </summary>
public interface IEcomCustomerRepository
{
    /// <summary>Reads the Ecom-lookup reference record for a fulfilment code, or <see langword="null"/> if none is configured.</summary>
    /// <param name="fulfilmentId">Fulfilment location to resolve the reference record for.</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    Task<EcomCustomer?> GetAsync(string fulfilmentId, CancellationToken cancellationToken = default);
}
