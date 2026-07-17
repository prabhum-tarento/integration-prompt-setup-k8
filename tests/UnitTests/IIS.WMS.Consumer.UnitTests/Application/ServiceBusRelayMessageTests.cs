using IIS.WMS.Common.Logging;
using IIS.WMS.Consumer.Application.Common;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Construction tests for the <see cref="ServiceBusRelayMessage"/> record.</summary>
public class ServiceBusRelayMessageTests
{
    [Fact(DisplayName = "Constructor with every optional argument supplied assigns every property")]
    public void Constructor_AllArgumentsSupplied_AssignsProperties()
    {
        var message = new ServiceBusRelayMessage(
            QueueName: "bulk-import",
            SessionId: "WH1:SKU1",
            MessageId: "msg-1",
            CorrelationId: "corr-1",
            AppId: "consumer-app",
            Types: ["BulkInventoryImport"],
            SourceName: "BulkInventoryImportConsumer",
            PayloadName: "InventoryBulkImportItem",
            Json: "{\"quantity\":10}",
            MaxMessageSizeBytesOverride: 128_000,
            LogCriteria: LogCriteria.High,
            EntityType: "InventoryBulkImportItem");

        Assert.Equal("bulk-import", message.QueueName);
        Assert.Equal("WH1:SKU1", message.SessionId);
        Assert.Equal("msg-1", message.MessageId);
        Assert.Equal("corr-1", message.CorrelationId);
        Assert.Equal("consumer-app", message.AppId);
        Assert.Equal(["BulkInventoryImport"], message.Types);
        Assert.Equal("BulkInventoryImportConsumer", message.SourceName);
        Assert.Equal("InventoryBulkImportItem", message.PayloadName);
        Assert.Equal("{\"quantity\":10}", message.Json);
        Assert.Equal(128_000, message.MaxMessageSizeBytesOverride);
        Assert.Equal(LogCriteria.High, message.LogCriteria);
        Assert.Equal("InventoryBulkImportItem", message.EntityType);
    }

    [Fact(DisplayName = "Constructor with optional arguments omitted defaults them to null")]
    public void Constructor_OptionalArgumentsOmitted_DefaultsToNull()
    {
        var message = new ServiceBusRelayMessage(
            QueueName: "bulk-import",
            SessionId: "WH1:SKU1",
            MessageId: "msg-1",
            CorrelationId: "corr-1",
            AppId: null,
            Types: null,
            SourceName: "BulkInventoryImportConsumer",
            PayloadName: "InventoryBulkImportItem",
            Json: "{\"quantity\":10}");

        Assert.Null(message.AppId);
        Assert.Null(message.Types);
        Assert.Null(message.MaxMessageSizeBytesOverride);
        Assert.Null(message.LogCriteria);
        Assert.Null(message.EntityType);
    }
}
