namespace IIS.WMS.Consumer.Domain.Exceptions;

/// <summary>
/// Raised when an internal-hallmarking operation targets an <c>ItemStockInventory</c> record that
/// does not exist for the given category - the application-level <c>MISSING_INVENTORY</c> rejection
/// (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.1/§8), distinct from a Cosmos `404`
/// at the repository boundary: the record is genuinely expected to exist (created at Goods Receipt),
/// so its absence is a business validation outcome the caller logs and skips the line for, not a
/// silent no-op.
/// </summary>
public sealed class MissingItemStockInventoryException : DomainException
{
    /// <summary>Builds the exception with a message identifying the missing record.</summary>
    /// <param name="id">Id of the <c>ItemStockInventory</c> record that was expected to exist.</param>
    /// <param name="itemCode">Item code the request was made against.</param>
    public MissingItemStockInventoryException(string id, string itemCode)
        : base($"No ItemStockInventory record found for '{id}' (item '{itemCode}').")
    {
        Id = id;
        ItemCode = itemCode;
    }

    /// <summary>Id of the <c>ItemStockInventory</c> record that was expected to exist.</summary>
    public string Id { get; }

    /// <summary>Item code the request was made against.</summary>
    public string ItemCode { get; }
}
