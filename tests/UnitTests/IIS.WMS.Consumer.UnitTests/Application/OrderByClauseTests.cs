using IIS.WMS.Consumer.Application.Common;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Factory-helper tests for <see cref="OrderByClause{T}"/> and its <see cref="OrderByClause"/> factory.</summary>
public class OrderByClauseTests
{
    private sealed record TestRow(string Name, int Rank);

    [Fact(DisplayName = "Asc builds an ascending clause with the given key selector")]
    public void Asc_KeySelector_BuildsAscendingClause()
    {
        var clause = OrderByClause.Asc<TestRow>(x => x.Rank);

        Assert.False(clause.Descending);
        var compiled = clause.KeySelector.Compile();
        Assert.Equal(3, compiled(new TestRow("A", 3)));
    }

    [Fact(DisplayName = "Desc builds a descending clause with the given key selector")]
    public void Desc_KeySelector_BuildsDescendingClause()
    {
        var clause = OrderByClause.Desc<TestRow>(x => x.Name);

        Assert.True(clause.Descending);
        var compiled = clause.KeySelector.Compile();
        Assert.Equal("A", compiled(new TestRow("A", 3)));
    }

    [Fact(DisplayName = "Two clauses built from the same selector and direction are equal - record value semantics")]
    public void Equals_SameSelectorInstanceAndDirection_AreEqual()
    {
        System.Linq.Expressions.Expression<Func<TestRow, object>> selector = x => x.Rank;

        var first = new OrderByClause<TestRow>(selector, Descending: false);
        var second = new OrderByClause<TestRow>(selector, Descending: false);

        Assert.Equal(first, second);
    }
}
