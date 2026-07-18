using System.Collections.Concurrent;

namespace IIS.WMS.Common.Messaging.ServiceBus;

/// <summary>
/// Process-wide registry of one <see cref="ServiceBusHealthState"/> per queue, keyed by queue name.
/// Replaces per-queue keyed DI registration (<c>AddKeyedSingleton&lt;ServiceBusHealthState&gt;</c>) so
/// that a hosted service and <see cref="ServiceBusHealthCheck"/> derive the same shared instance from
/// the one piece of runtime data they already both have - the queue name - instead of needing a
/// separately-injected, separately-keyed parameter that merely has to agree with it.
/// </summary>
public sealed class ServiceBusHealthStateRegistry
{
    private readonly ConcurrentDictionary<string, ServiceBusHealthState> states = new();

    /// <summary>Returns the shared <see cref="ServiceBusHealthState"/> for <paramref name="queueName"/>, creating it on first access.</summary>
    /// <param name="queueName">The queue this state tracks.</param>
    public ServiceBusHealthState GetOrAdd(string queueName) => states.GetOrAdd(queueName, static _ => new ServiceBusHealthState());
}
