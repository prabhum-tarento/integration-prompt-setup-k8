namespace IIS.WMS.Consumer.Domain.Exceptions;

/// <summary>
/// Raised when a pick or unpick would take an <c>ItemStockInventory</c> quantity below zero and no
/// fallback (e.g. B2C extension borrowing) applies - the oversell-prevention invariant every stock
/// decrement must honor (see dotnet-architecture-good-practices.instructions.md §5).
/// </summary>
public sealed class InsufficientItemStockException : DomainException
{
    /// <summary>Builds the exception with a message summarizing the shortfall.</summary>
    /// <param name="id">Id of the <c>ItemStockInventory</c> record the request was made against.</param>
    /// <param name="itemCode">Item code the request was made against.</param>
    /// <param name="requested">Quantity that was requested.</param>
    /// <param name="available">Quantity actually available at the time of the request.</param>
    public InsufficientItemStockException(string id, string itemCode, int requested, int available)
        : base($"Cannot apply {requested} unit(s) to item '{itemCode}' on stock record '{id}': " +
               $"only {available} available.")
    {
        Id = id;
        ItemCode = itemCode;
        Requested = requested;
        Available = available;
    }

    /// <summary>Id of the <c>ItemStockInventory</c> record the request was made against.</summary>
    public string Id { get; }

    /// <summary>Item code the request was made against.</summary>
    public string ItemCode { get; }

    /// <summary>Quantity that was requested.</summary>
    public int Requested { get; }

    /// <summary>Quantity actually available at the time of the request.</summary>
    public int Available { get; }
}
