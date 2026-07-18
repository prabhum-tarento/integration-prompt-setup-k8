using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="InventoryStateChangedServiceBusConsumerOptions.ApplyServiceBusLevelDefaults"/> -
/// the queue-level-first, ServiceBus-level-fallback merge for <c>MaxConcurrentSessions</c> and
/// <c>MaxConcurrentCallsPerSession</c>.
/// </summary>
public class InventoryStateChangedServiceBusConsumerOptionsTests
{
    [Fact(DisplayName = "Unset queue-level settings fall back to the ServiceBus-level values")]
    public void ApplyServiceBusLevelDefaults_EventLevelUnset_FallsBackToServiceBusLevel()
    {
        var serviceBusLevel = new ServiceBusConsumerOptions
        {
            MaxConcurrentSessions = 8,
            MaxConcurrentCallsPerSession = 1,
        };
        var eventLevel = new InventoryStateChangedServiceBusConsumerOptions();

        eventLevel.ApplyServiceBusLevelDefaults(serviceBusLevel);

        Assert.Equal(8, eventLevel.MaxConcurrentSessions);
        Assert.Equal(1, eventLevel.MaxConcurrentCallsPerSession);
    }

    [Fact(DisplayName = "Configured queue-level settings win over the ServiceBus-level values")]
    public void ApplyServiceBusLevelDefaults_EventLevelConfigured_EventLevelWins()
    {
        var serviceBusLevel = new ServiceBusConsumerOptions
        {
            MaxConcurrentSessions = 8,
            MaxConcurrentCallsPerSession = 1,
        };
        var eventLevel = new InventoryStateChangedServiceBusConsumerOptions
        {
            MaxConcurrentSessions = 16,
            MaxConcurrentCallsPerSession = 2,
        };

        eventLevel.ApplyServiceBusLevelDefaults(serviceBusLevel);

        Assert.Equal(16, eventLevel.MaxConcurrentSessions);
        Assert.Equal(2, eventLevel.MaxConcurrentCallsPerSession);
    }

    [Fact(DisplayName = "A partially configured queue level only falls back for the settings it left unset")]
    public void ApplyServiceBusLevelDefaults_EventLevelPartiallyConfigured_MergesPerSetting()
    {
        var serviceBusLevel = new ServiceBusConsumerOptions
        {
            MaxConcurrentSessions = 8,
            MaxConcurrentCallsPerSession = 1,
        };
        var eventLevel = new InventoryStateChangedServiceBusConsumerOptions
        {
            MaxConcurrentSessions = 16,
        };

        eventLevel.ApplyServiceBusLevelDefaults(serviceBusLevel);

        Assert.Equal(16, eventLevel.MaxConcurrentSessions);
        Assert.Equal(1, eventLevel.MaxConcurrentCallsPerSession);
    }
}
