using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.UnitTests.Common;

/// <summary>
/// Correctness tests for <see cref="ServiceBusMessageOutcome"/> - the settlement-agnostic result of
/// handling one received message (integration-resiliency.instructions.md §9).
/// </summary>
public class ServiceBusMessageOutcomeTests
{
    [Fact(DisplayName = "The Completed singleton carries the Completed kind and no reason/description")]
    public void Completed_StaticInstance_HasCompletedKindAndNoReason()
    {
        Assert.Equal(ServiceBusMessageOutcomeKind.Completed, ServiceBusMessageOutcome.Completed.Kind);
        Assert.Null(ServiceBusMessageOutcome.Completed.Reason);
        Assert.Null(ServiceBusMessageOutcome.Completed.Description);
    }

    [Fact(DisplayName = "The Abandoned singleton carries the Abandoned kind and no reason/description")]
    public void Abandoned_StaticInstance_HasAbandonedKindAndNoReason()
    {
        Assert.Equal(ServiceBusMessageOutcomeKind.Abandoned, ServiceBusMessageOutcome.Abandoned.Kind);
        Assert.Null(ServiceBusMessageOutcome.Abandoned.Reason);
        Assert.Null(ServiceBusMessageOutcome.Abandoned.Description);
    }

    [Fact(DisplayName = "DeadLettered(reason) sets the DeadLettered kind and reason, with no description")]
    public void DeadLettered_ReasonOnly_SetsReasonWithoutDescription()
    {
        var outcome = ServiceBusMessageOutcome.DeadLettered("PoisonMessage");

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("PoisonMessage", outcome.Reason);
        Assert.Null(outcome.Description);
    }

    [Fact(DisplayName = "DeadLettered(reason, description) sets both the reason and the description")]
    public void DeadLettered_ReasonAndDescription_SetsBoth()
    {
        var outcome = ServiceBusMessageOutcome.DeadLettered("PoisonMessage", "Deserialization failed");

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("PoisonMessage", outcome.Reason);
        Assert.Equal("Deserialization failed", outcome.Description);
    }

    [Fact(DisplayName = "Two DeadLettered outcomes built from the same reason/description are equal, per record value semantics")]
    public void Equals_SameReasonAndDescription_ReturnsTrue()
    {
        var first = ServiceBusMessageOutcome.DeadLettered("PoisonMessage", "Deserialization failed");
        var second = ServiceBusMessageOutcome.DeadLettered("PoisonMessage", "Deserialization failed");

        Assert.Equal(first, second);
    }
}
