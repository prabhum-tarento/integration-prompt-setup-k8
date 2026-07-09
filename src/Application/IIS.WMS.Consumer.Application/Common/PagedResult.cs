namespace IIS.WMS.Consumer.Application.Common;

/// <summary>A page of results from a Cosmos continuation-token-based query (cosmos-db.instructions.md §7). Never built from an in-memory Skip/Take over a full result set.</summary>
public sealed class PagedResult<T>
{
    /// <summary>Items returned in this page.</summary>
    public IReadOnlyList<T> Items { get; init; } = [];

    /// <summary>Token to pass as <see cref="QueryOptions{T}.ContinuationToken"/> to fetch the next page, or <see langword="null"/> if this was the last page.</summary>
    public string? ContinuationToken { get; init; }

    /// <summary>Number of items in this page - equal to <c>Items.Count</c>.</summary>
    public int Count { get; init; }
}
