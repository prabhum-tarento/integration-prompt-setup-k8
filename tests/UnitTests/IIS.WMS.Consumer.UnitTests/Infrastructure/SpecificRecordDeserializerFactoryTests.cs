using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;
using Microsoft.Extensions.Logging;
using NSubstitute;
using net.pandora.nexus.@event.inventory;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="SpecificRecordDeserializerFactory"/> - the reusable Avro/Schema
/// Registry wiring <see cref="IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.InventoryStateChangedConsumerHostedService"/>
/// builds its Kafka value deserializer from.
/// </summary>
public class SpecificRecordDeserializerFactoryTests
{
    [Fact(DisplayName = "Create returns a usable deserializer and hands back its own disposable Schema Registry client")]
    public void Create_ValidSchemaRegistryUrl_ReturnsDeserializerAndDisposableClient()
    {
        var factory = new SpecificRecordDeserializerFactory(Substitute.For<ILogger<SpecificRecordDeserializerFactory>>());

        var deserializer = factory.Create<InventoryStateChanged>("http://localhost:8081", out var schemaRegistryClient);

        Assert.NotNull(deserializer);
        Assert.NotNull(schemaRegistryClient);

        schemaRegistryClient.Dispose();
    }

    [Fact(DisplayName = "Create is called once per consumer, so each call returns an independent Schema Registry client")]
    public void Create_CalledTwice_ReturnsIndependentSchemaRegistryClients()
    {
        var factory = new SpecificRecordDeserializerFactory(Substitute.For<ILogger<SpecificRecordDeserializerFactory>>());

        factory.Create<InventoryStateChanged>("http://localhost:8081", out var first);
        factory.Create<InventoryStateChanged>("http://localhost:8081", out var second);

        Assert.NotSame(first, second);

        first.Dispose();
        second.Dispose();
    }
}
