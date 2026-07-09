namespace IIS.WMS.Consumer.Domain.Exceptions;

/// <summary>
/// Raised when a reservation or allocation would take on-hand quantity negative - the
/// oversell-prevention invariant every stock decrement must honor (see
/// dotnet-architecture-good-practices.instructions.md §5).
/// </summary>
public sealed class InsufficientStockException : DomainException
{
    /// <summary>Builds the exception with a message summarizing the shortfall.</summary>
    /// <param name="warehouseId">Warehouse the request was made against.</param>
    /// <param name="sku">SKU the request was made against.</param>
    /// <param name="requested">Quantity that was requested.</param>
    /// <param name="available">Quantity actually on hand at the time of the request.</param>
    public InsufficientStockException(string warehouseId, string sku, int requested, int available)
        : base($"Cannot reserve {requested} unit(s) of SKU '{sku}' at warehouse '{warehouseId}': " +
               $"only {available} on hand.")
    {
        WarehouseId = warehouseId;
        Sku = sku;
        Requested = requested;
        Available = available;
    }

    /// <summary>Warehouse the request was made against.</summary>
    public string WarehouseId { get; }

    /// <summary>SKU the request was made against.</summary>
    public string Sku { get; }

    /// <summary>Quantity that was requested.</summary>
    public int Requested { get; }

    /// <summary>Quantity actually on hand at the time of the request.</summary>
    public int Available { get; }
}
