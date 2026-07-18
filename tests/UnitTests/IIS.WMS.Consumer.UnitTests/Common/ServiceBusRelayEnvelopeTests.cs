using System.Text.Json;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;

namespace IIS.WMS.Consumer.UnitTests.Common;

/// <summary>
/// Correctness tests for <see cref="ServiceBusRelayEnvelope"/> - the body-level envelope the Kafka-side
/// relay wraps every schema's payload inside (integration-resiliency.instructions.md §4).
/// </summary>
public class ServiceBusRelayEnvelopeTests
{
    [Fact(DisplayName = "A freshly constructed envelope exposes null reference properties, a default ReflexSchema, and an empty BlobPath")]
    public void Constructor_NoPropertiesSet_ExposesDefaults()
    {
        var envelope = new ServiceBusRelayEnvelope();

        Assert.Null(envelope.CorrelationId);
        Assert.Null(envelope.AppId);
        Assert.Null(envelope.LogCriteria);
        Assert.Null(envelope.EntityType);
        Assert.Null(envelope.Type);
        Assert.Equal(default, envelope.ReflexSchema);
        Assert.Equal(string.Empty, envelope.BlobPath);
    }

    [Fact(DisplayName = "Object-initializer syntax sets every init-only property")]
    public void ObjectInitializer_AllPropertiesSet_ExposesConfiguredValues()
    {
        var reflexSchema = JsonSerializer.SerializeToElement(new { Sku = "SKU1" });

        var envelope = new ServiceBusRelayEnvelope
        {
            CorrelationId = "corr-1",
            AppId = "app-1",
            LogCriteria = LogCriteria.High,
            EntityType = "InventoryEvent",
            Type = "InventoryStateChanged",
            ReflexSchema = reflexSchema,
            BlobPath = "large-payload/corr-1.json",
        };

        Assert.Equal("corr-1", envelope.CorrelationId);
        Assert.Equal("app-1", envelope.AppId);
        Assert.Equal(LogCriteria.High, envelope.LogCriteria);
        Assert.Equal("InventoryEvent", envelope.EntityType);
        Assert.Equal("InventoryStateChanged", envelope.Type);
        Assert.Equal(JsonValueKind.Object, envelope.ReflexSchema.ValueKind);
        Assert.Equal("large-payload/corr-1.json", envelope.BlobPath);
    }

    [Fact(DisplayName = "BlobPath, unlike the other members, can be assigned after construction")]
    public void BlobPath_AssignedAfterConstruction_UpdatesValue()
    {
        var envelope = new ServiceBusRelayEnvelope();

        envelope.BlobPath = "large-payload/corr-2.json";

        Assert.Equal("large-payload/corr-2.json", envelope.BlobPath);
    }

    [Fact(DisplayName = "ReflexSchema, like BlobPath, can be assigned after construction")]
    public void ReflexSchema_AssignedAfterConstruction_UpdatesValue()
    {
        var envelope = new ServiceBusRelayEnvelope { BlobPath = "large-payload/corr-3.json" };
        var rehydratedSchema = JsonSerializer.SerializeToElement(new { Sku = "SKU2" });

        envelope.ReflexSchema = rehydratedSchema;

        Assert.Equal(JsonValueKind.Object, envelope.ReflexSchema.ValueKind);
    }
}
