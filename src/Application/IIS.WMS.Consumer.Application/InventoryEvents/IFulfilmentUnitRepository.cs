using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>Port for <see cref="FulfilmentUnit"/> master-data lookups (§3.7 OMS delta market resolution, docs/InventoryStateChangedFullQueueTrigger.md).</summary>
public interface IFulfilmentUnitRepository
{
    /// <summary>Reads the fulfilment unit record for a fulfilment id, or <see langword="null"/> if it doesn't exist.</summary>
    /// <param name="fulfilmentId">Fulfilment location id.</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    Task<FulfilmentUnit?> GetByFulfilmentIdAsync(string fulfilmentId, CancellationToken cancellationToken = default);
}
