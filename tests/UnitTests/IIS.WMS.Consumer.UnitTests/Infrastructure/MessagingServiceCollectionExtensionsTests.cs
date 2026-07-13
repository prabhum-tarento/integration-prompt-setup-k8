using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Infrastructure.Messaging;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="MessagingServiceCollectionExtensions.IsFunctionEnabled"/> - the
/// <c>Kafka:Functions</c> allow-list filter, mirroring an Azure Functions host's <c>functions</c>
/// startup filter rather than a per-consumer <c>Enabled</c> flag.
/// </summary>
public class MessagingServiceCollectionExtensionsTests
{
    [Fact(DisplayName = "A null filter allows every consumer")]
    public void IsFunctionEnabled_NullFilter_ReturnsTrue()
    {
        Assert.True(MessagingServiceCollectionExtensions.IsFunctionEnabled(null, KafkaEvents.InventoryAdjustedEventType));
    }

    [Fact(DisplayName = "An empty filter allows every consumer")]
    public void IsFunctionEnabled_EmptyFilter_ReturnsTrue()
    {
        Assert.True(MessagingServiceCollectionExtensions.IsFunctionEnabled([], KafkaEvents.InventoryAdjustedEventType));
    }

    [Fact(DisplayName = "A filter naming this consumer allows it")]
    public void IsFunctionEnabled_FilterContainsConsumer_ReturnsTrue()
    {
        var filter = new[] { KafkaEvents.InventoryAdjustedEventType };

        Assert.True(MessagingServiceCollectionExtensions.IsFunctionEnabled(filter, KafkaEvents.InventoryAdjustedEventType));
    }

    [Fact(DisplayName = "A filter naming only a different consumer excludes this one")]
    public void IsFunctionEnabled_FilterExcludesConsumer_ReturnsFalse()
    {
        var filter = new[] { KafkaEvents.InventoryAdjustedEventType };

        Assert.False(MessagingServiceCollectionExtensions.IsFunctionEnabled(filter, KafkaEvents.InventoryEventsConsumerKey));
    }

    [Fact(DisplayName = "The filter match is case-insensitive")]
    public void IsFunctionEnabled_FilterDifferentCase_ReturnsTrue()
    {
        var filter = new[] { "inventoryevents" };

        Assert.True(MessagingServiceCollectionExtensions.IsFunctionEnabled(filter, KafkaEvents.InventoryEventsConsumerKey));
    }
}
