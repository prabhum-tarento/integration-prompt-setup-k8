using IIS.WMS.Consumer.Application.Common;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Construction/default tests for <see cref="PagedResult{T}"/>.</summary>
public class PagedResultTests
{
    [Fact(DisplayName = "A default-constructed page has an empty item list and no continuation token")]
    public void Construct_NoInitializers_DefaultsToEmptyItemsAndNullContinuationToken()
    {
        var page = new PagedResult<string>();

        Assert.Empty(page.Items);
        Assert.Null(page.ContinuationToken);
        Assert.Equal(0, page.Count);
    }

    [Fact(DisplayName = "Object initializers assign every property")]
    public void Construct_WithInitializers_AssignsProperties()
    {
        var page = new PagedResult<string>
        {
            Items = ["a", "b"],
            ContinuationToken = "token-1",
            Count = 2,
        };

        Assert.Equal(["a", "b"], page.Items);
        Assert.Equal("token-1", page.ContinuationToken);
        Assert.Equal(2, page.Count);
    }
}
