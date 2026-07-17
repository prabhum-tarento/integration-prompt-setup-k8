using Azure.Messaging.ServiceBus;
using IIS.WMS.Common.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.UnitTests.Common;

/// <summary>
/// Correctness tests for <see cref="ServiceBusConsumerOptions"/> - bound from the
/// <c>ServiceBus</c> configuration section.
/// </summary>
public class ServiceBusConsumerOptionsTests
{
    [Fact(DisplayName = "A freshly constructed instance exposes the documented defaults")]
    public void Constructor_NoOverrides_ExposesDefaults()
    {
        var options = new ServiceBusConsumerOptions();

        Assert.Equal("inventory-events", options.QueueName);
        Assert.Equal(ServiceBusTransportType.AmqpTcp, options.TransportType);
        Assert.NotNull(options.Retry);
    }

    [Fact(DisplayName = "Every settable property can be overridden via object initializer")]
    public void ObjectInitializer_AllPropertiesSet_ExposesConfiguredValues()
    {
        var retry = new ServiceBusRetryOptions { MaxRetries = 7 };

        var options = new ServiceBusConsumerOptions
        {
            ConnectionString = "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y",
            QueueName = "custom-queue",
            TransportType = ServiceBusTransportType.AmqpWebSockets,
            Retry = retry,
        };

        Assert.Equal("Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y", options.ConnectionString);
        Assert.Equal("custom-queue", options.QueueName);
        Assert.Equal(ServiceBusTransportType.AmqpWebSockets, options.TransportType);
        Assert.Same(retry, options.Retry);
    }
}
