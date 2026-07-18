using IIS.WMS.Consumer.Application.Common;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Construction/default tests for <see cref="QueryOptions{T}"/> and its projection variant <see cref="QueryOptions{T, TResult}"/>.</summary>
public class QueryOptionsTests
{
    private sealed record TestRow(string Id, int Quantity);

    [Fact(DisplayName = "QueryOptions defaults PageSize to 20 and every other property to null/false")]
    public void Construct_NoInitializers_DefaultsPageSizeAndOtherPropertiesToUnset()
    {
        var options = new QueryOptions<TestRow>();

        Assert.Null(options.Predicate);
        Assert.Null(options.OrderBy);
        Assert.Equal(20, options.PageSize);
        Assert.Null(options.ContinuationToken);
        Assert.Null(options.Category);
        Assert.False(options.AllowCrossPartitionScan);
    }

    [Fact(DisplayName = "QueryOptions property initializers assign every property")]
    public void Construct_WithInitializers_AssignsProperties()
    {
        var orderBy = new[] { OrderByClause.Asc<TestRow>(x => x.Quantity) };

        var options = new QueryOptions<TestRow>
        {
            Predicate = x => x.Quantity > 0,
            OrderBy = orderBy,
            PageSize = 50,
            ContinuationToken = "token-1",
            Category = "WH1:SKU1",
            AllowCrossPartitionScan = true,
        };

        Assert.NotNull(options.Predicate);
        Assert.Same(orderBy, options.OrderBy);
        Assert.Equal(50, options.PageSize);
        Assert.Equal("token-1", options.ContinuationToken);
        Assert.Equal("WH1:SKU1", options.Category);
        Assert.True(options.AllowCrossPartitionScan);
    }

    [Fact(DisplayName = "Projection QueryOptions defaults PageSize to 20 and every other property to null/false")]
    public void ConstructProjection_NoInitializers_DefaultsPageSizeAndOtherPropertiesToUnset()
    {
        var options = new QueryOptions<TestRow, string>
        {
            Selector = x => x.Id,
        };

        Assert.Null(options.Predicate);
        Assert.Null(options.OrderBy);
        Assert.Equal(20, options.PageSize);
        Assert.Null(options.ContinuationToken);
        Assert.Null(options.Category);
        Assert.False(options.AllowCrossPartitionScan);
    }

    [Fact(DisplayName = "Projection QueryOptions property initializers assign every property including the selector")]
    public void ConstructProjection_WithInitializers_AssignsProperties()
    {
        var orderBy = new[] { OrderByClause.Desc<TestRow>(x => x.Quantity) };

        var options = new QueryOptions<TestRow, string>
        {
            Predicate = x => x.Quantity > 0,
            Selector = x => x.Id,
            OrderBy = orderBy,
            PageSize = 10,
            ContinuationToken = "token-2",
            Category = "WH2:SKU2",
            AllowCrossPartitionScan = true,
        };

        Assert.NotNull(options.Predicate);
        Assert.NotNull(options.Selector);
        Assert.Same(orderBy, options.OrderBy);
        Assert.Equal(10, options.PageSize);
        Assert.Equal("token-2", options.ContinuationToken);
        Assert.Equal("WH2:SKU2", options.Category);
        Assert.True(options.AllowCrossPartitionScan);
    }
}
