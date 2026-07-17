namespace IIS.WMS.Common.Messaging.ServiceBus;

/// <summary>Shared singleton the hosted service updates on every message received - available for a future staleness-based health check alongside the current reachability-based one.</summary>
public sealed class ServiceBusHealthState
{
    /// <summary>UTC timestamp of the most recent message successfully received from the session processor.</summary>
    public DateTimeOffset LastSuccessfulReceiveUtc { get; set; } = DateTimeOffset.UtcNow;
}
