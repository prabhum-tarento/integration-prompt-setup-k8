using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Application-layer service for recalculating B2C extension and OMS delta metrics when processing
/// pick/unpick events on extended-inventory records. Abstracts the calculation logic from infrastructure
/// for testability and dependency injection.
/// </summary>
public interface IItemStockInventoryExtensionCalculationService
{
    /// <summary>
    /// Recalculates B2C extension values and OMS delta metrics when a pick/unpick event is processed
    /// on an extended-inventory aggregate. Updates the aggregate's B2CExtended and B2CAvailable values
    /// in place, and populates <paramref name="deltaResult"/> with the change metrics.
    /// </summary>
    /// <param name="prevB2CAvailable">The B2C available quantity before the pick/unpick was applied.</param>
    /// <param name="itemStockInventory">The aggregate to update in place.</param>
    /// <param name="deltaResult">Output object to populate with <c>IsB2CChanged</c> and delta amount.</param>
    /// <param name="cancellationToken">Token to cancel any async repository operations.</param>
    Task CalculateB2CExtensionAsync(
        int prevB2CAvailable,
        ItemStockInventory itemStockInventory,
        ItemStockInventoryDeltaResult deltaResult,
        CancellationToken cancellationToken = default);
}
