using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Round-trip tests for <see cref="MessageArchiveMapper"/> - unlike <c>OrderArchiveMapper</c>, no JSON
/// parse/serialize step is involved since <see cref="MessageArchive.Payload"/> is already a plain string
/// on both sides.
/// </summary>
public class MessageArchiveMapperTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact(DisplayName = "ToDocument projects every aggregate property verbatim")]
    public void ToDocument_ValidAggregate_ProjectsEveryProperty()
    {
        var aggregate = MessageArchive.Create("id-1", "InventoryStateChanged", "{\"sku\":\"SKU1\"}", "corr-1", Now);
        aggregate.ETag = "etag-value";

        var document = MessageArchiveMapper.ToDocument(aggregate);

        Assert.Equal(aggregate.Id, document.Id);
        Assert.Equal(aggregate.Category, document.Category);
        Assert.Equal(aggregate.Payload, document.Payload);
        Assert.Equal(aggregate.CorrelationId, document.CorrelationId);
        Assert.Equal(aggregate.Timestamp, document.Timestamp);
        Assert.Equal(aggregate.ETag, document.ETag);
    }

    [Fact(DisplayName = "ToDomain rehydrates every document property verbatim, carrying the ETag along")]
    public void ToDomain_ValidDocument_RehydratesEveryPropertyAndETag()
    {
        var document = new MessageArchiveDocument
        {
            Id = "id-1",
            Category = "InventoryStateChanged",
            Payload = "{\"sku\":\"SKU1\"}",
            CorrelationId = "corr-1",
            Timestamp = Now,
            ETag = "etag-value",
        };

        var aggregate = MessageArchiveMapper.ToDomain(document);

        Assert.Equal(document.Id, aggregate.Id);
        Assert.Equal(document.Category, aggregate.Category);
        Assert.Equal(document.Payload, aggregate.Payload);
        Assert.Equal(document.CorrelationId, aggregate.CorrelationId);
        Assert.Equal(document.Timestamp, aggregate.Timestamp);
        Assert.Equal(document.ETag, aggregate.ETag);
    }

    [Fact(DisplayName = "ToDocument followed by ToDomain round-trips every property")]
    public void RoundTrip_ToDocumentThenToDomain_PreservesEveryProperty()
    {
        var original = MessageArchive.Create("id-1", "InventoryStateChanged", "{\"sku\":\"SKU1\"}", "corr-1", Now);
        original.ETag = "etag-value";

        var roundTripped = MessageArchiveMapper.ToDomain(MessageArchiveMapper.ToDocument(original));

        Assert.Equal(original.Id, roundTripped.Id);
        Assert.Equal(original.Category, roundTripped.Category);
        Assert.Equal(original.Payload, roundTripped.Payload);
        Assert.Equal(original.CorrelationId, roundTripped.CorrelationId);
        Assert.Equal(original.Timestamp, roundTripped.Timestamp);
        Assert.Equal(original.ETag, roundTripped.ETag);
    }
}
