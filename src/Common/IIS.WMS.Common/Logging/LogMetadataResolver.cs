using System.Collections.Concurrent;
using System.Reflection;

namespace IIS.WMS.Common.Logging;

/// <summary>
/// Resolves a consumer class's <see cref="LogLevelCriteriaAttribute"/>/<see cref="ModuleAttribute"/>
/// declarations via reflection, once per concrete type - shared by every hosted-service consumer
/// (Kafka and Service Bus alike, in both the Consumer and the planned Producer project) rather than
/// each re-implementing the same lookup/caching. A consumer missing either attribute falls back to
/// <see cref="LogCriteria.Default"/>/its own type name, so decorating a consumer class remains optional.
/// </summary>
public static class LogMetadataResolver
{
    private static readonly ConcurrentDictionary<Type, (LogCriteria LogLevel, string Module)> Cache = new();

    public static (LogCriteria LogLevel, string Module) Resolve(Type consumerType) =>
        Cache.GetOrAdd(consumerType, static type => (
            type.GetCustomAttribute<LogLevelCriteriaAttribute>()?.LogLevel ?? LogCriteria.Default,
            type.GetCustomAttribute<ModuleAttribute>()?.Name ?? type.Name));
}
