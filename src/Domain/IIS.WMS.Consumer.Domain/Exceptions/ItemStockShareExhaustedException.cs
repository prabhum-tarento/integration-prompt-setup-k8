namespace IIS.WMS.Consumer.Domain.Exceptions;

/// <summary>
/// Raised when a B2C pick on an extended item would borrow more than the remaining
/// <c>B2BUsedShare</c> - the floor on how much B2B share a B2C oversell may consume (see
/// dotnet-architecture-good-practices.instructions.md §5).
/// </summary>
public sealed class ItemStockShareExhaustedException : DomainException
{
    /// <summary>Builds the exception with a message summarizing the shortfall.</summary>
    /// <param name="id">Id of the <c>ItemStockInventory</c> record the request was made against.</param>
    /// <param name="itemCode">Item code the request was made against.</param>
    /// <param name="requested">B2B used-share quantity that would need to be borrowed.</param>
    /// <param name="availableShare">B2B used-share quantity actually remaining at the time of the request.</param>
    public ItemStockShareExhaustedException(string id, string itemCode, int requested, int availableShare)
        : base($"Cannot borrow {requested} unit(s) of B2B used-share for item '{itemCode}' on stock record '{id}': " +
               $"only {availableShare} share remaining.")
    {
        Id = id;
        ItemCode = itemCode;
        Requested = requested;
        AvailableShare = availableShare;
    }

    /// <summary>Id of the <c>ItemStockInventory</c> record the request was made against.</summary>
    public string Id { get; }

    /// <summary>Item code the request was made against.</summary>
    public string ItemCode { get; }

    /// <summary>B2B used-share quantity that would need to be borrowed.</summary>
    public int Requested { get; }

    /// <summary>B2B used-share quantity actually remaining at the time of the request.</summary>
    public int AvailableShare { get; }
}
