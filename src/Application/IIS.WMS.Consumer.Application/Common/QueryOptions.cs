using System.Linq.Expressions;

namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// Filtering/paging options for a repository query. The implementation must throw if
/// <see cref="Category"/> is null and <see cref="AllowCrossPartitionScan"/> is false
/// (cosmos-db.instructions.md §6) - this is what makes "minimize cross-partition queries" a real
/// guardrail instead of an aspiration.
/// </summary>
public class QueryOptions<T>
{
    /// <summary>Filter applied to the query; <see langword="null"/> means no filter.</summary>
    public Expression<Func<T, bool>>? Predicate { get; set; }

    /// <summary>
    /// Multi-key sort - see <see cref="OrderByClause{T}"/>. Applied in list order (the first clause
    /// is the primary sort key, each following clause breaks ties left by the ones before it);
    /// <see langword="null"/>/empty means no explicit ordering.
    /// </summary>
    public IReadOnlyList<OrderByClause<T>>? OrderBy { get; set; }

    /// <summary>Maximum number of items to return in one page.</summary>
    public int PageSize { get; set; } = 20;

    /// <summary>Continuation token from a previous page's <c>PagedResult</c>, or <see langword="null"/> to start from the beginning.</summary>
    public string? ContinuationToken { get; set; }

    /// <summary>Partition key to scope the query to. Required unless <see cref="AllowCrossPartitionScan"/> is explicitly set.</summary>
    public string? Category { get; set; }

    /// <summary>Explicit opt-in to a cross-partition scan when <see cref="Category"/> is not supplied - makes the RU-cost tradeoff a visible decision, not an accident.</summary>
    public bool AllowCrossPartitionScan { get; set; }
}

/// <summary>Projection variant of <see cref="QueryOptions{T}"/> - same filtering/paging/guardrail fields, plus a required <see cref="Selector"/>.</summary>
public class QueryOptions<T, TResult>
{
    /// <summary>Filter applied to the query; <see langword="null"/> means no filter.</summary>
    public Expression<Func<T, bool>>? Predicate { get; set; }

    /// <summary>Projection applied to each matching item before it's returned.</summary>
    public Expression<Func<T, TResult>> Selector { get; set; } = default!;

    /// <summary>
    /// Multi-key sort - see <see cref="OrderByClause{T}"/>. Applied in list order (the first clause
    /// is the primary sort key, each following clause breaks ties left by the ones before it);
    /// <see langword="null"/>/empty means no explicit ordering.
    /// </summary>
    public IReadOnlyList<OrderByClause<T>>? OrderBy { get; set; }

    /// <summary>Maximum number of items to return in one page.</summary>
    public int PageSize { get; set; } = 20;

    /// <summary>Continuation token from a previous page's <c>PagedResult</c>, or <see langword="null"/> to start from the beginning.</summary>
    public string? ContinuationToken { get; set; }

    /// <summary>Partition key to scope the query to. Required unless <see cref="AllowCrossPartitionScan"/> is explicitly set.</summary>
    public string? Category { get; set; }

    /// <summary>Explicit opt-in to a cross-partition scan when <see cref="Category"/> is not supplied - makes the RU-cost tradeoff a visible decision, not an accident.</summary>
    public bool AllowCrossPartitionScan { get; set; }
}
