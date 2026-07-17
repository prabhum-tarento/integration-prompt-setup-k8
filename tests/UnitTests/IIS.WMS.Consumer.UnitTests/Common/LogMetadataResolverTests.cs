using IIS.WMS.Common.Logging;

namespace IIS.WMS.Consumer.UnitTests.Common;

/// <summary>
/// Correctness tests for <see cref="LogMetadataResolver"/> - the reflection-based, per-type cached
/// lookup of a consumer class's <see cref="LogLevelCriteriaAttribute"/>/<see cref="ModuleAttribute"/>
/// declarations, falling back to <see cref="LogCriteria.Default"/>/the type's own name when either
/// attribute is missing.
/// </summary>
public class LogMetadataResolverTests
{
    [Fact(DisplayName = "A type decorated with both attributes resolves both configured values")]
    public void Resolve_BothAttributesPresent_ReturnsConfiguredValues()
    {
        var (logLevel, module) = LogMetadataResolver.Resolve(typeof(FullyDecoratedConsumer));

        Assert.Equal(LogCriteria.High, logLevel);
        Assert.Equal("Inventory", module);
    }

    [Fact(DisplayName = "A type decorated with only LogLevelCriteriaAttribute falls back to its own type name for Module")]
    public void Resolve_OnlyLogLevelCriteriaAttribute_FallsBackToTypeNameForModule()
    {
        var (logLevel, module) = LogMetadataResolver.Resolve(typeof(LogLevelOnlyConsumer));

        Assert.Equal(LogCriteria.Medium, logLevel);
        Assert.Equal(nameof(LogLevelOnlyConsumer), module);
    }

    [Fact(DisplayName = "A type decorated with only ModuleAttribute falls back to LogCriteria.Default")]
    public void Resolve_OnlyModuleAttribute_FallsBackToDefaultLogLevel()
    {
        var (logLevel, module) = LogMetadataResolver.Resolve(typeof(ModuleOnlyConsumer));

        Assert.Equal(LogCriteria.Default, logLevel);
        Assert.Equal("BulkImport", module);
    }

    [Fact(DisplayName = "A type decorated with neither attribute falls back to LogCriteria.Default and its own type name")]
    public void Resolve_NeitherAttributePresent_FallsBackToDefaults()
    {
        var (logLevel, module) = LogMetadataResolver.Resolve(typeof(UndecoratedConsumer));

        Assert.Equal(LogCriteria.Default, logLevel);
        Assert.Equal(nameof(UndecoratedConsumer), module);
    }

    [Fact(DisplayName = "Resolving the same type twice returns the same cached values")]
    public void Resolve_SameTypeResolvedTwice_ReturnsSameValuesBothTimes()
    {
        var first = LogMetadataResolver.Resolve(typeof(FullyDecoratedConsumer));
        var second = LogMetadataResolver.Resolve(typeof(FullyDecoratedConsumer));

        Assert.Equal(first, second);
    }

    [LogLevelCriteria(LogCriteria.High)]
    [Module("Inventory")]
    private sealed class FullyDecoratedConsumer;

    [LogLevelCriteria(LogCriteria.Medium)]
    private sealed class LogLevelOnlyConsumer;

    [Module("BulkImport")]
    private sealed class ModuleOnlyConsumer;

    private sealed class UndecoratedConsumer;
}
