using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.UnitTests.Common;

/// <summary>
/// Correctness tests for <see cref="BulkImportServiceBusConsumerOptions"/> - bound from the
/// <c>ServiceBus:BulkInventoryImport</c> configuration section, for the non-session bulk-import queue.
/// </summary>
public class BulkImportServiceBusConsumerOptionsTests
{
    [Fact(DisplayName = "A freshly constructed instance exposes the documented defaults")]
    public void Constructor_NoOverrides_ExposesDefaults()
    {
        var options = new BulkImportServiceBusConsumerOptions();

        Assert.Equal("inventory-bulk-import", options.QueueName);
        Assert.Equal(32, options.MaxConcurrentCalls);
    }

    [Fact(DisplayName = "Every settable property can be overridden via object initializer")]
    public void ObjectInitializer_AllPropertiesSet_ExposesConfiguredValues()
    {
        var options = new BulkImportServiceBusConsumerOptions
        {
            QueueName = "custom-bulk-import",
            MaxConcurrentCalls = 64,
        };

        Assert.Equal("custom-bulk-import", options.QueueName);
        Assert.Equal(64, options.MaxConcurrentCalls);
    }
}
