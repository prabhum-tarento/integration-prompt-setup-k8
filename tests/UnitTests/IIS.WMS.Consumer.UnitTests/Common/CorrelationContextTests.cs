using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Logging;

namespace IIS.WMS.Consumer.UnitTests.Common;

/// <summary>
/// Correctness tests for <see cref="CorrelationContext"/> - the mutable
/// <see cref="ICorrelationContext"/> implementation set once at the HTTP or message-consumer
/// boundary (integration-resiliency.instructions.md §4).
/// </summary>
public class CorrelationContextTests
{
    [Fact(DisplayName = "A freshly constructed context exposes empty/default values")]
    public void Constructor_NoSetCalled_ExposesDefaults()
    {
        var context = new CorrelationContext();

        Assert.Equal(string.Empty, context.CorrelationId);
        Assert.Equal(string.Empty, context.AppId);
        Assert.Equal(string.Empty, context.Type);
        Assert.Empty(context.Types);
        Assert.Equal(LogCriteria.Default, context.LogLevel);
        Assert.Equal(string.Empty, context.Module);
    }

    [Fact(DisplayName = "Set(correlationId) sets only the correlation id, leaving every other member at its default")]
    public void Set_CorrelationIdOnly_SetsCorrelationIdOnly()
    {
        var context = new CorrelationContext();

        context.Set("corr-1");

        Assert.Equal("corr-1", context.CorrelationId);
        Assert.Equal(string.Empty, context.AppId);
        Assert.Equal(string.Empty, context.Type);
        Assert.Empty(context.Types);
        Assert.Equal(LogCriteria.Default, context.LogLevel);
        Assert.Equal(string.Empty, context.Module);
    }

    [Fact(DisplayName = "Set(correlationId, appId, types) sets the three Kafka-boundary members, leaving log metadata at its default")]
    public void Set_ThreeArgOverload_SetsCorrelationAppIdAndTypes()
    {
        var context = new CorrelationContext();

        context.Set("corr-1", "app-1", ["InventoryStateChanged", "InventoryConsumer"]);

        Assert.Equal("corr-1", context.CorrelationId);
        Assert.Equal("app-1", context.AppId);
        Assert.Equal(["InventoryStateChanged", "InventoryConsumer"], context.Types);
        Assert.Equal("InventoryStateChanged", context.Type);
        Assert.Equal(LogCriteria.Default, context.LogLevel);
        Assert.Equal(string.Empty, context.Module);
    }

    [Fact(DisplayName = "Type returns an empty string when Types is empty")]
    public void Type_TypesEmpty_ReturnsEmptyString()
    {
        var context = new CorrelationContext();

        context.Set("corr-1", "app-1", []);

        Assert.Equal(string.Empty, context.Type);
    }

    [Fact(DisplayName = "Type returns the first entry when Types has more than one element")]
    public void Type_MultipleTypes_ReturnsFirstEntry()
    {
        var context = new CorrelationContext();

        context.Set("corr-1", "app-1", ["First", "Second", "Third"]);

        Assert.Equal("First", context.Type);
    }

    [Fact(DisplayName = "Set(correlationId, appId, types, logLevel, module) sets every member")]
    public void Set_FiveArgOverload_SetsEveryMember()
    {
        var context = new CorrelationContext();

        context.Set("corr-1", "app-1", ["InventoryStateChanged"], LogCriteria.High, "Inventory");

        Assert.Equal("corr-1", context.CorrelationId);
        Assert.Equal("app-1", context.AppId);
        Assert.Equal(["InventoryStateChanged"], context.Types);
        Assert.Equal("InventoryStateChanged", context.Type);
        Assert.Equal(LogCriteria.High, context.LogLevel);
        Assert.Equal("Inventory", context.Module);
    }

    [Fact(DisplayName = "A later Set call overwrites values from an earlier one")]
    public void Set_CalledTwice_LaterCallOverwritesEarlierValues()
    {
        var context = new CorrelationContext();

        context.Set("corr-1", "app-1", ["First"], LogCriteria.High, "Inventory");
        context.Set("corr-2", "app-2", ["Second"], LogCriteria.Low, "BulkImport");

        Assert.Equal("corr-2", context.CorrelationId);
        Assert.Equal("app-2", context.AppId);
        Assert.Equal(["Second"], context.Types);
        Assert.Equal(LogCriteria.Low, context.LogLevel);
        Assert.Equal("BulkImport", context.Module);
    }
}
