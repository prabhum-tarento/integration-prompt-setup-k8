using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;
using Microsoft.Extensions.Logging;
using NSubstitute;
using net.pandora.nexus.@event.inventory;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="SpecificRecordDeserializerFactory"/> - the reusable Avro/Schema
/// Registry wiring <see cref="IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.InventoryStateChangedConsumerHostedService"/>
/// builds its Kafka value deserializer from.
/// </summary>
public class SpecificRecordDeserializerFactoryTests
{
    [Fact(DisplayName = "Create returns a usable deserializer and hands back its own disposable Schema Registry client")]
    public void Create_ValidSchemaRegistryUrl_ReturnsDeserializerAndDisposableClient()
    {
        var factory = new SpecificRecordDeserializerFactory(Substitute.For<ILogger<SpecificRecordDeserializerFactory>>());

        var deserializer = factory.Create<InventoryStateChanged>("http://localhost:8081", null, null, out var schemaRegistryClient);

        Assert.NotNull(deserializer);
        Assert.NotNull(schemaRegistryClient);

        schemaRegistryClient.Dispose();
    }

    [Fact(DisplayName = "Create is called once per consumer, so each call returns an independent Schema Registry client")]
    public void Create_CalledTwice_ReturnsIndependentSchemaRegistryClients()
    {
        var factory = new SpecificRecordDeserializerFactory(Substitute.For<ILogger<SpecificRecordDeserializerFactory>>());

        factory.Create<InventoryStateChanged>("http://localhost:8081", null, null, out var first);
        factory.Create<InventoryStateChanged>("http://localhost:8081", null, null, out var second);

        Assert.NotSame(first, second);

        first.Dispose();
        second.Dispose();
    }

    [Fact(DisplayName = "Create with an API key and secret still returns a usable deserializer and client")]
    public void Create_WithApiKeyAndSecret_ReturnsDeserializerAndDisposableClient()
    {
        var factory = new SpecificRecordDeserializerFactory(Substitute.For<ILogger<SpecificRecordDeserializerFactory>>());

        var deserializer = factory.Create<InventoryStateChanged>(
            "https://psrc-example.westeurope.azure.confluent.cloud", "api-key", "api-secret", out var schemaRegistryClient);

        Assert.NotNull(deserializer);
        Assert.NotNull(schemaRegistryClient);

        schemaRegistryClient.Dispose();
    }

    [Theory(DisplayName = "Create with only one half of the API key/secret pair leaves the registry client unauthenticated, same as neither being set")]
    [InlineData("api-key", null)]
    [InlineData(null, "api-secret")]
    [InlineData("", "")]
    public void Create_WithOnlyOneCredentialHalf_StillReturnsDeserializerAndDisposableClient(string? apiKey, string? apiSecret)
    {
        var factory = new SpecificRecordDeserializerFactory(Substitute.For<ILogger<SpecificRecordDeserializerFactory>>());

        var deserializer = factory.Create<InventoryStateChanged>("http://localhost:8081", apiKey, apiSecret, out var schemaRegistryClient);

        Assert.NotNull(deserializer);
        Assert.NotNull(schemaRegistryClient);

        schemaRegistryClient.Dispose();
    }

    [Fact(DisplayName = "Create with an already-built Schema Registry client returns a usable deserializer without opening a second client")]
    public void Create_WithExistingSchemaRegistryClient_ReturnsDeserializerForThatClient()
    {
        var factory = new SpecificRecordDeserializerFactory(Substitute.For<ILogger<SpecificRecordDeserializerFactory>>());

        var first = factory.Create<InventoryStateChanged>("http://localhost:8081", null, null, out var schemaRegistryClient);
        var second = factory.Create<InventoryAdjusted>(schemaRegistryClient);

        Assert.NotNull(first);
        Assert.NotNull(second);

        schemaRegistryClient.Dispose();
    }
}
