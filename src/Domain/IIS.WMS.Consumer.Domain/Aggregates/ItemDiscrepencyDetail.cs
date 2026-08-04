using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// §5.4 write-only discrepancy audit record (docs/events/inventory.StockSyncSubmitted.md) - written
/// when a stock sync's reported pickable quantity disagrees with the pre-update <c>B2CAVL</c>
/// (<c>IISAvlQty != avlPickableQnty</c>). Like <see cref="MessageArchive"/>, this aggregate has no
/// invariants beyond its required fields and raises no domain events.
/// </summary>
public sealed class ItemDiscrepencyDetail : AggregateRoot
{
    /// <summary>Fulfilment code this discrepancy was recorded under - also this entity's Cosmos partition key.</summary>
    public string Category { get; private init; } = default!;

    public string ItemCode { get; private init; } = default!;

    public string CountryOfOrigin { get; private init; } = default!;

    public string Hallmark { get; private init; } = default!;

    /// <summary>The pre-update <c>B2CAVL</c> value (the doc's <c>IISAvlQty</c>).</summary>
    public int IISAvlQty { get; private init; }

    /// <summary>The reported pickable quantity (the doc's <c>avlPickableQnty</c>).</summary>
    public int ReflexAvlQty { get; private init; }

    public bool MasterDataExists { get; private init; }

    public string FulfilmentCode { get; private init; } = default!;

    /// <summary>
    /// Opaque optimistic-concurrency token populated by the repository. Not guarded on - the
    /// Infrastructure repository upserts unconditionally, since this is a write-once diagnostic record,
    /// not concurrently contested state.
    /// </summary>
    public string? ETag { get; set; }

    /// <summary>Parameterless so the object initializers in <see cref="Create"/> and <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private ItemDiscrepencyDetail()
    {
    }

    /// <summary>
    /// Creates a new discrepancy record. <paramref name="id"/> is the caller's own deterministic
    /// identifier, so a redelivered message naturally upserts rather than duplicating.
    /// </summary>
    public static ItemDiscrepencyDetail Create(
        string id, string itemCode, string countryOfOrigin, string hallmark,
        int iisAvlQty, int reflexAvlQty, bool masterDataExists, string fulfilmentCode)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(fulfilmentCode);

        return new ItemDiscrepencyDetail
        {
            Id = id,
            Category = fulfilmentCode,
            ItemCode = itemCode,
            CountryOfOrigin = countryOfOrigin,
            Hallmark = hallmark,
            IISAvlQty = iisAvlQty,
            ReflexAvlQty = reflexAvlQty,
            MasterDataExists = masterDataExists,
            FulfilmentCode = fulfilmentCode,
        };
    }

    /// <summary>Rehydrates an aggregate from persisted state - the repository mapper's entry point, not for new items.</summary>
    public static ItemDiscrepencyDetail Rehydrate(
        string id, string category, string itemCode, string countryOfOrigin, string hallmark,
        int iisAvlQty, int reflexAvlQty, bool masterDataExists, string fulfilmentCode) => new()
    {
        Id = id,
        Category = category,
        ItemCode = itemCode,
        CountryOfOrigin = countryOfOrigin,
        Hallmark = hallmark,
        IISAvlQty = iisAvlQty,
        ReflexAvlQty = reflexAvlQty,
        MasterDataExists = masterDataExists,
        FulfilmentCode = fulfilmentCode,
    };
}
