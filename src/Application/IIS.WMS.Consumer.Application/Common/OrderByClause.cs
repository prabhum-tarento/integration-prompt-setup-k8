using System.Linq.Expressions;

namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// One key in a multi-column sort (cosmos-db.instructions.md §8) - <see cref="QueryOptions{T}.OrderBy"/>/
/// <see cref="QueryOptions{T,TResult}.OrderBy"/> take an ordered list of these, applied as
/// <c>OrderBy(...).ThenBy(...)</c>/<c>OrderByDescending(...).ThenByDescending(...)</c> in list order -
/// the first clause is the primary sort key, each clause after it breaks ties left by the ones before it.
/// </summary>
/// <param name="KeySelector">The property to sort by.</param>
/// <param name="Descending">Whether this key sorts descending instead of ascending.</param>
public sealed record OrderByClause<T>(Expression<Func<T, object>> KeySelector, bool Descending);

/// <summary>Factory helpers for <see cref="OrderByClause{T}"/> - reads at the call site as <c>OrderByClause.Asc&lt;T&gt;(x => x.Id)</c>.</summary>
public static class OrderByClause
{
    /// <summary>Sorts ascending by <paramref name="keySelector"/>.</summary>
    public static OrderByClause<T> Asc<T>(Expression<Func<T, object>> keySelector) => new(keySelector, Descending: false);

    /// <summary>Sorts descending by <paramref name="keySelector"/>.</summary>
    public static OrderByClause<T> Desc<T>(Expression<Func<T, object>> keySelector) => new(keySelector, Descending: true);
}
