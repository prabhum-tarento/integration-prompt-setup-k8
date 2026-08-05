using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;
using Microsoft.Extensions.Caching.Memory;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Caching;

/// <summary>
/// <see cref="IMemoryCache"/>-backed decorator over <see cref="EcomCustomerRepository"/> (Q2/Q4) -
/// this Ecom-lookup reference record rarely changes, so caching it in-process avoids a Cosmos
/// round-trip on every consolidated-shipment message.
/// </summary>
public sealed class CachedEcomCustomerRepository(
    EcomCustomerRepository innerRepository, IMemoryCache cache) : IEcomCustomerRepository
{
    private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(15);

    public async Task<EcomCustomer?> GetAsync(string fulfilmentId, CancellationToken cancellationToken = default)
    {
        var cacheKey = $"{nameof(EcomCustomer)}:{fulfilmentId}";

        if (cache.TryGetValue(cacheKey, out EcomCustomer? cached))
        {
            return cached;
        }

        var customer = await innerRepository.GetByFulfilmentIdAsync(fulfilmentId, cancellationToken);
        cache.Set(cacheKey, customer, CacheDuration);

        return customer;
    }
}
