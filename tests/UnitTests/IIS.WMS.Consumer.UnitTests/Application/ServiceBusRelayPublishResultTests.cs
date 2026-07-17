using IIS.WMS.Consumer.Application.Common;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Construction tests for the <see cref="ServiceBusRelayPublishResult"/> record.</summary>
public class ServiceBusRelayPublishResultTests
{
    [Fact(DisplayName = "Constructor for an offloaded message assigns every property")]
    public void Constructor_OffloadedMessage_AssignsProperties()
    {
        var result = new ServiceBusRelayPublishResult(
            WasOffloaded: true,
            BlobPath: "bulk-import/InventoryBulkImportItem/msg-1.json",
            BlobOffloadDuration: TimeSpan.FromMilliseconds(42),
            PublishDuration: TimeSpan.FromMilliseconds(7));

        Assert.True(result.WasOffloaded);
        Assert.Equal("bulk-import/InventoryBulkImportItem/msg-1.json", result.BlobPath);
        Assert.Equal(TimeSpan.FromMilliseconds(42), result.BlobOffloadDuration);
        Assert.Equal(TimeSpan.FromMilliseconds(7), result.PublishDuration);
    }

    [Fact(DisplayName = "Constructor for an inline message has no blob path or offload duration")]
    public void Constructor_InlineMessage_HasNoBlobOffload()
    {
        var result = new ServiceBusRelayPublishResult(
            WasOffloaded: false,
            BlobPath: string.Empty,
            BlobOffloadDuration: TimeSpan.Zero,
            PublishDuration: TimeSpan.FromMilliseconds(5));

        Assert.False(result.WasOffloaded);
        Assert.Equal(string.Empty, result.BlobPath);
        Assert.Equal(TimeSpan.Zero, result.BlobOffloadDuration);
    }
}
