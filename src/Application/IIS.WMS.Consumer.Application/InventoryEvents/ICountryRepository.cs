using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>Port for <see cref="CountryMaster"/> master-data lookups (§3.7 OMS delta market validation, docs/events/inventory.InventoryStateChanged.md).</summary>
public interface ICountryRepository
{
    /// <summary>Reads the country master record for a country/market code, or <see langword="null"/> if it doesn't exist.</summary>
    /// <param name="countryCode">Country/market code.</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    Task<CountryMaster?> GetByCodeAsync(string countryCode, CancellationToken cancellationToken = default);
}
