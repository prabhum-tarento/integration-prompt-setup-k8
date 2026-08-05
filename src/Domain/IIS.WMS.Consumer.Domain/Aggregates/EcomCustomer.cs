using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// Reference data consulted by the DEECOMDC e-commerce engraving workflow to decide whether a
/// resolved <see cref="OrderTracking.CustomerId"/> should trigger engraving stock updates
/// (docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.3 step 1). One document per fulfilment
/// code, held in the shared <c>MasterData</c> container and cached in-memory by the repository
/// decorator to avoid a DB round-trip on every message.
/// </summary>
public sealed class EcomCustomer : AggregateRoot
{
    /// <summary>The composite key - matches the Cosmos partition key.</summary>
    public string Category { get; private init; } = default!;

    /// <summary>Fulfilment location this reference record applies to.</summary>
    public string FulfilmentId { get; private init; } = default!;

    /// <summary>Allow-list of customer identifiers that resolve to the DEECOMDC engraving customer.</summary>
    public IReadOnlyCollection<string> EcomDcList { get; private init; } = [];

    /// <summary>TDC customer identifier that also resolves to the DEECOMDC engraving customer.</summary>
    public string? TdcCustomerId { get; private init; }

    /// <summary>Parameterless so the object initializer in <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private EcomCustomer()
    {
    }

    /// <summary>Builds the deterministic id/partition key for one fulfilment code's Ecom-lookup reference record.</summary>
    public static string BuildId(string fulfilmentId) => $"EcomCustomer_{fulfilmentId}".ToUpperInvariant();

    /// <summary>Rehydrates an aggregate from persisted state - the repository mapper's entry point.</summary>
    public static EcomCustomer Rehydrate(
        string id, string category, string fulfilmentId, IReadOnlyCollection<string> ecomDcList, string? tdcCustomerId) => new()
    {
        Id = id,
        Category = category,
        FulfilmentId = fulfilmentId,
        EcomDcList = ecomDcList,
        TdcCustomerId = tdcCustomerId,
    };

    /// <summary>True when <paramref name="customerId"/> is in the allow-list or matches the TDC customer id (§3.3 step 1).</summary>
    public bool Matches(string customerId) =>
        EcomDcList.Contains(customerId, StringComparer.OrdinalIgnoreCase) ||
        string.Equals(TdcCustomerId, customerId, StringComparison.OrdinalIgnoreCase);
}
