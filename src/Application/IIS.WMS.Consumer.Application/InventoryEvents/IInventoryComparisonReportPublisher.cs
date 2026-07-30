namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// §3.8 Inventory Comparison Report (ICR) snapshot publisher port
/// (docs/events/inventory.InventoryStateChanged.md) - ported from the upstream Reflex facade's
/// <c>inventoryComparisonReportEventHandlerAsync</c>. Implemented in the Infrastructure layer
/// (<c>InventoryComparisonReportPublisher</c>) since it depends on Service Bus queue configuration
/// (<c>InventoryPublishOptions</c>) - this port only exposes Domain/Application-shaped parameters.
/// </summary>
public interface IInventoryComparisonReportPublisher
{
    /// <summary>
    /// Fetches the current (post-mutation) <c>ItemStockInventory</c> record for the given key and, if
    /// found, publishes a 4-entry B2B_AVL/B2C_AVL/B2B_PREP/B2C_PREP snapshot. A missing record is a
    /// silent no-op, matching Reflex's own behavior of not publishing when there's nothing to report.
    /// </summary>
    /// <param name="fulfilmentId">Fulfilment location id.</param>
    /// <param name="itemCode">Item code being reported.</param>
    /// <param name="hallmark">Hallmarking value of the item line.</param>
    /// <param name="countryOfOrigin">Country of origin of the item line.</param>
    /// <param name="isThirdPartyLogistics">Whether the fulfilment location is the CAECOM (3PL) location - drives the outbound <c>Location.Type</c>.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task PublishAsync(
        string fulfilmentId, string itemCode, string hallmark, string countryOfOrigin,
        bool isThirdPartyLogistics, CancellationToken cancellationToken = default);
}
