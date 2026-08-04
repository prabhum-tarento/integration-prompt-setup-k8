namespace IIS.WMS.Consumer.Domain.Exceptions;

/// <summary>
/// Raised when an internal-hallmarking quantity mutation would take a bucket negative, or supplies a
/// zero allocation - the application-level <c>INVALID_QUANTITY</c> rejection
/// (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.1/§3.3/§8). Kept strictly distinct
/// from Cosmos concurrency/duplicate signals (<c>409</c>/<c>412</c>) per that doc's own explicit
/// warning - this is a Domain-owned business invariant, not an infrastructure conflict.
/// </summary>
public sealed class InvalidItemStockInventoryQtyException : DomainException
{
    /// <summary>Builds the exception with a message summarizing the rejected quantity.</summary>
    /// <param name="id">Id of the <c>ItemStockInventory</c> record the request was made against.</param>
    /// <param name="itemCode">Item code the request was made against.</param>
    /// <param name="requested">Quantity that was requested.</param>
    /// <param name="resultingValue">The quantity the mutation would have produced.</param>
    public InvalidItemStockInventoryQtyException(string id, string itemCode, int requested, int resultingValue)
        : base($"Cannot apply {requested} unit(s) to item '{itemCode}' on stock record '{id}': " +
               $"resulting quantity {resultingValue} is invalid.")
    {
        Id = id;
        ItemCode = itemCode;
        Requested = requested;
        ResultingValue = resultingValue;
    }

    /// <summary>Id of the <c>ItemStockInventory</c> record the request was made against.</summary>
    public string Id { get; }

    /// <summary>Item code the request was made against.</summary>
    public string ItemCode { get; }

    /// <summary>Quantity that was requested.</summary>
    public int Requested { get; }

    /// <summary>The quantity the mutation would have produced.</summary>
    public int ResultingValue { get; }
}
