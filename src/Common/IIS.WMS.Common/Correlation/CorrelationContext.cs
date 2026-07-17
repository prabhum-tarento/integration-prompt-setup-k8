using IIS.WMS.Common.Logging;

namespace IIS.WMS.Common.Correlation;

/// <inheritdoc cref="ICorrelationContext"/>
public sealed class CorrelationContext : ICorrelationContext
{
    /// <inheritdoc />
    public string CorrelationId { get; private set; } = string.Empty;

    /// <inheritdoc />
    public string AppId { get; private set; } = string.Empty;

    /// <inheritdoc />
    public string Type => Types.FirstOrDefault() ?? string.Empty;

    public IReadOnlyList<string> Types { get; private set; } = [];

    /// <inheritdoc />
    public LogCriteria LogLevel { get; private set; } = LogCriteria.Default;

    /// <inheritdoc />
    public string Module { get; private set; } = string.Empty;

    /// <inheritdoc />
    public void Set(string correlationId)
    {
        CorrelationId = correlationId;
    }

    /// <inheritdoc />
    public void Set(string correlationId, string appId, IReadOnlyList<string> types)
    {
        CorrelationId = correlationId;
        AppId = appId;
        Types = types;
    }

    /// <inheritdoc />
    public void Set(string correlationId, string appId, IReadOnlyList<string> types, LogCriteria logLevel, string module)
    {
        CorrelationId = correlationId;
        AppId = appId;
        Types = types;
        LogLevel = logLevel;
        Module = module;
    }
}
