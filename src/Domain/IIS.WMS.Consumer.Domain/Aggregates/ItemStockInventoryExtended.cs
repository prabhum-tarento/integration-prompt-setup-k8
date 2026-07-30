using IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// §3.5 extended-state inventory snapshot (docs/InventoryStateChangedFullQueueTrigger.md) - tracks a
/// quantity sitting in a non-standard (State, Status) pair, keyed distinctly per pair rather than
/// merged with the baseline <see cref="ItemStockInventory"/> record.
/// </summary>
public sealed class ItemStockInventoryExtended
{
    public string ItemCode { get; set; } = default!;
    public string FulfilmentId { get; set; } = default!;
    public string COO { get; set; } = default!;
    public string Hallmark { get; set; } = default!;
    public State State { get; set; }
    public Status Status { get; set; }
    public int? Qty { get; set; }
    public DateTime? SubmittedDate { get; set; }
    public bool? IsPOSM { get; set; }
    public string? ETag { get; set; }

    /// <summary>The composite id/partition-key value for one (FulfilmentId, ItemCode, Hallmark, COO, State, Status) combination - mirrors <see cref="ItemStockInventory.BuildId"/>'s convention.</summary>
    public string Id => BuildId(FulfilmentId, ItemCode, Hallmark, COO, State, Status);

    public static string BuildId(
        string fulfilmentId, string itemCode, string hallmark, string countryOfOrigin, State state, Status status) =>
        $"{fulfilmentId}:{itemCode}:{hallmark}:{countryOfOrigin}:{state}:{status}".ToUpperInvariant();
}
