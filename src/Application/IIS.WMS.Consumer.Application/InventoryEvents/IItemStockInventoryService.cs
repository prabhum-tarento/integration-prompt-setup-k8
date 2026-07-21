using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Use-case orchestration for <see cref="Domain.Aggregates.ItemStockInventory"/>: loads/persists via
/// <see cref="IItemStockInventoryRepository"/>, invokes the aggregate's pick/unpick business methods,
/// and dispatches the domain events they raise. Ported from the upstream Reflex facade's
/// <c>InventoryPickEventHandler</c>/<c>InventoryUnpickEventHandler</c> - this is the interface
/// <c>InventoryStateChangedHandler</c> depends on, it never touches the repository or Cosmos types
/// directly.
/// </summary>
public interface IItemStockInventoryService
{
    /// <summary>
    /// Applies a pick against the identified fulfilment/item/hallmark/COO record, retrying against
    /// fresh state on an ETag conflict (integration-resiliency.instructions.md §2) - the fix for the
    /// "PreCondition failed" issue this port addresses. If no such record exists, logs a warning and
    /// returns without mutating (mirrors Reflex's own missing-record reject). If the pick would
    /// oversell with no fallback available, logs a warning and returns without mutating - this is a
    /// tolerated business condition, not a poison message.
    /// </summary>
    /// <param name="fulfilmentId">Fulfilment location the pick occurred at.</param>
    /// <param name="itemCode">Item code being picked.</param>
    /// <param name="countryOfOrigin">Country of origin of the item line.</param>
    /// <param name="hallmark">Hallmarking value of the item line.</param>
    /// <param name="channel">Whether this is a B2B or B2C pick.</param>
    /// <param name="quantity">Quantity picked.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task ApplyPickAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        ItemStockPickChannel channel, int quantity, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies an unpick against the identified fulfilment/item/hallmark/COO record, retrying against
    /// fresh state on an ETag conflict, with the same missing-record and oversell handling as
    /// <see cref="ApplyPickAsync"/>.
    /// </summary>
    /// <param name="fulfilmentId">Fulfilment location the unpick occurred at.</param>
    /// <param name="itemCode">Item code being unpicked.</param>
    /// <param name="countryOfOrigin">Country of origin of the item line.</param>
    /// <param name="hallmark">Hallmarking value of the item line.</param>
    /// <param name="quantity">Quantity unpicked.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task ApplyUnpickAsync(
        string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark,
        int quantity, CancellationToken cancellationToken = default);
}
