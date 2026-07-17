namespace IIS.WMS.Common.Logging;

/// <summary>
/// Declares a message consumer class's log verbosity criteria - resolved via reflection
/// (<see cref="Type.GetCustomAttribute"/>) once per consumer type and pushed onto
/// <c>ICorrelationContext.LogLevel</c> and the Serilog <c>LogContext</c> at the start of every
/// message this consumer processes.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class LogLevelCriteriaAttribute : Attribute
{
    public LogCriteria LogLevel { get; }

    public LogLevelCriteriaAttribute(LogCriteria logLevel)
    {
        LogLevel = logLevel;
    }
}

/// <summary>
/// Declares a message consumer class's business-domain module (e.g. "Inventory", "BulkImport") - a
/// coarser grouping than the consumer's own class name, resolved and pushed the same way as
/// <see cref="LogLevelCriteriaAttribute"/>.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class ModuleAttribute : Attribute
{
    public string Name { get; }

    public ModuleAttribute(string name)
    {
        Name = name;
    }
}
