using IIS.WMS.Common.Logging;

namespace IIS.WMS.Consumer.UnitTests.Common;

/// <summary>
/// Correctness tests for <see cref="LogLevelCriteriaAttribute"/> - the class-level attribute a
/// message consumer declares its log verbosity criteria with, resolved via
/// <see cref="LogMetadataResolver"/>.
/// </summary>
public class LogLevelCriteriaAttributeTests
{
    [Theory(DisplayName = "The constructor exposes the supplied LogCriteria value unchanged")]
    [InlineData(LogCriteria.Default)]
    [InlineData(LogCriteria.High)]
    [InlineData(LogCriteria.Medium)]
    [InlineData(LogCriteria.Low)]
    public void Constructor_GivenLogCriteria_ExposesItOnLogLevel(LogCriteria logCriteria)
    {
        var attribute = new LogLevelCriteriaAttribute(logCriteria);

        Assert.Equal(logCriteria, attribute.LogLevel);
    }

    [Fact(DisplayName = "The attribute is applied to and readable off a decorated class via reflection")]
    public void GetCustomAttribute_ClassDecorated_ReturnsConfiguredLogLevel()
    {
        var attribute = typeof(HighCriteriaConsumer).GetCustomAttributes(typeof(LogLevelCriteriaAttribute), inherit: false)
            .Cast<LogLevelCriteriaAttribute>()
            .Single();

        Assert.Equal(LogCriteria.High, attribute.LogLevel);
    }

    [LogLevelCriteria(LogCriteria.High)]
    private sealed class HighCriteriaConsumer;
}
