namespace IIS.WMS.Common.Logging;

/// <summary>Log verbosity criteria for a message consumer - declared per consumer class via <see cref="LogLevelCriteriaAttribute"/>.</summary>
public enum LogCriteria
{
    Default,
    High,
    Medium,
    Low,
}
