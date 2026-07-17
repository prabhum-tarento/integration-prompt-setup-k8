using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.UnitTests.Common;

/// <summary>
/// Correctness tests for <see cref="ServiceBusHealthState"/> - the shared singleton a hosted service
/// updates on every message received.
/// </summary>
public class ServiceBusHealthStateTests
{
    [Fact(DisplayName = "A freshly constructed state's LastSuccessfulReceiveUtc defaults to close to the current time")]
    public void Constructor_NoValueAssigned_DefaultsToNearUtcNow()
    {
        var before = DateTimeOffset.UtcNow;

        var state = new ServiceBusHealthState();

        var after = DateTimeOffset.UtcNow;
        Assert.InRange(state.LastSuccessfulReceiveUtc, before, after);
    }

    [Fact(DisplayName = "LastSuccessfulReceiveUtc can be updated after construction")]
    public void LastSuccessfulReceiveUtc_AssignedAfterConstruction_UpdatesValue()
    {
        var state = new ServiceBusHealthState();
        var updated = new DateTimeOffset(2026, 7, 16, 0, 0, 0, TimeSpan.Zero);

        state.LastSuccessfulReceiveUtc = updated;

        Assert.Equal(updated, state.LastSuccessfulReceiveUtc);
    }
}
