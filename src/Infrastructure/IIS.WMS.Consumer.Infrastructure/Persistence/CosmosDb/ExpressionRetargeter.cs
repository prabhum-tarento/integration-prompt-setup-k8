using System.Linq.Expressions;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>
/// Rebinds a LINQ expression written against the Domain aggregate (<c>QueryOptions&lt;T&gt;</c>'s
/// <c>Predicate</c>/<c>OrderBy</c>/<c>Selector</c> in cosmos-db.instructions.md §6, §8) onto the
/// Cosmos persistence document, so the repository can keep the Application-facing interface typed
/// against the Domain aggregate while still building a query the Cosmos LINQ provider can
/// translate against <see cref="InventoryEventDocument"/>. Works because every property the
/// Domain aggregate exposes has an identically-named property on the document - see
/// <see cref="InventoryEventMapper"/>.
/// </summary>
internal static class ExpressionRetargeter
{
    /// <summary>Rebuilds <paramref name="expression"/> with its parameter type swapped from <typeparamref name="TSource"/> to <typeparamref name="TTarget"/>.</summary>
    /// <typeparam name="TSource">The type the expression was originally written against (the Domain aggregate).</typeparam>
    /// <typeparam name="TTarget">The type to retarget onto (the persistence document) - must expose an identically-named property for every member the expression references.</typeparam>
    /// <typeparam name="TResult">The expression's result type (e.g. <see langword="bool"/> for a predicate).</typeparam>
    /// <param name="expression">The expression to retarget.</param>
    /// <returns>An equivalent expression whose parameter is of type <typeparamref name="TTarget"/>.</returns>
    public static Expression<Func<TTarget, TResult>> Retarget<TSource, TTarget, TResult>(
        Expression<Func<TSource, TResult>> expression)
    {
        var newParameter = Expression.Parameter(typeof(TTarget), expression.Parameters[0].Name);
        var visitor = new RetargetingVisitor(expression.Parameters[0], newParameter, typeof(TTarget));
        var body = visitor.Visit(expression.Body)!;

        return Expression.Lambda<Func<TTarget, TResult>>(body, newParameter);
    }

    /// <summary>Walks an expression tree, replacing the old parameter and every member access on it with the equivalent member on the target type.</summary>
    private sealed class RetargetingVisitor(
        ParameterExpression oldParameter, ParameterExpression newParameter, Type targetType) : ExpressionVisitor
    {
        /// <summary>Replaces the source parameter reference with the target parameter.</summary>
        protected override Expression VisitParameter(ParameterExpression node) =>
            node == oldParameter ? newParameter : base.VisitParameter(node);

        /// <summary>Replaces a member access on the source parameter (e.g. <c>x.WarehouseId</c>) with the same-named member on the target type.</summary>
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression != oldParameter)
            {
                return base.VisitMember(node);
            }

            var targetProperty = targetType.GetProperty(node.Member.Name)
                ?? throw new InvalidOperationException(
                    $"'{targetType.Name}' has no property named '{node.Member.Name}' to retarget the query onto.");

            return Expression.Property(newParameter, targetProperty);
        }
    }
}
