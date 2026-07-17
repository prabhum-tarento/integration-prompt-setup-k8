using IIS.WMS.Common.Logging;

namespace IIS.WMS.Consumer.UnitTests.Common;

/// <summary>
/// Correctness tests for <see cref="ModuleAttribute"/> - declared alongside
/// <see cref="LogLevelCriteriaAttribute"/> in <c>Logging/LogLevelCriteriaAttribute.cs</c> (there is no
/// separate <c>ModuleAttribute.cs</c> file in this project), the class-level attribute a message
/// consumer declares its business-domain module with, resolved via <see cref="LogMetadataResolver"/>.
/// </summary>
public class ModuleAttributeTests
{
    [Theory(DisplayName = "The constructor exposes the supplied name unchanged")]
    [InlineData("Inventory")]
    [InlineData("BulkImport")]
    [InlineData("")]
    public void Constructor_GivenName_ExposesItOnName(string name)
    {
        var attribute = new ModuleAttribute(name);

        Assert.Equal(name, attribute.Name);
    }

    [Fact(DisplayName = "The attribute is applied to and readable off a decorated class via reflection")]
    public void GetCustomAttribute_ClassDecorated_ReturnsConfiguredName()
    {
        var attribute = typeof(InventoryModuleConsumer).GetCustomAttributes(typeof(ModuleAttribute), inherit: false)
            .Cast<ModuleAttribute>()
            .Single();

        Assert.Equal("Inventory", attribute.Name);
    }

    [Module("Inventory")]
    private sealed class InventoryModuleConsumer;
}
