using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Extended wrapper around <see cref="IItemStockInventoryService"/> that handles B2C extension
/// recalculation and OMS delta metrics for pick/unpick events on extended-inventory records.
/// Ported from the upstream Reflex facade's orchestrator logic.
/// </summary>
public interface IItemStockInventoryExtensionService
{
    /// <summary>
    /// Applies a B2B pick and recalculates B2C extension metrics if the record participates in extension.
    /// </summary>
    /// <param name="fulfilmentId">Fulfilment location the pick occurred at.</param>
    /// <param name="itemCode">Item code being picked.</param>
    /// <param name="countryOfOrigin">Country of origin of the item line.</param>
    /// <param name="hallmark">Hallmarking value of the item line.</param>
    /// <param name="quantity">Quantity picked.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>OMS delta metrics (zero values if no extension or no delta occurred).</returns>
    Task<ItemStockInventoryDeltaResult> ApplyPickB2BWithExtensionAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a B2C pick and recalculates B2C extension metrics if the record participates in extension.
    /// </summary>
    /// <param name="fulfilmentId">Fulfilment location the pick occurred at.</param>
    /// <param name="itemCode">Item code being picked.</param>
    /// <param name="countryOfOrigin">Country of origin of the item line.</param>
    /// <param name="hallmark">Hallmarking value of the item line.</param>
    /// <param name="quantity">Quantity picked.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>OMS delta metrics (zero values if no extension or no delta occurred).</returns>
    Task<ItemStockInventoryDeltaResult> ApplyPickB2CWithExtensionAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies an unpick and recalculates B2C extension metrics if the record participates in extension.
    /// </summary>
    /// <param name="fulfilmentId">Fulfilment location the unpick occurred at.</param>
    /// <param name="itemCode">Item code being unpicked.</param>
    /// <param name="countryOfOrigin">Country of origin of the item line.</param>
    /// <param name="hallmark">Hallmarking value of the item line.</param>
    /// <param name="quantity">Quantity unpicked.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>OMS delta metrics (zero values if no extension or no delta occurred).</returns>
    Task<ItemStockInventoryDeltaResult> ApplyUnpickWithExtensionAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default);
}
