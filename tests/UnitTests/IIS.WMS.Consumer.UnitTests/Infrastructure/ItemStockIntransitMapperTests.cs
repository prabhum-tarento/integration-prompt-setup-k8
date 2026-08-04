using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="ItemStockIntransitMapper"/> - the hand-written mapping between the
/// <see cref="ItemStockIntransit"/> Domain aggregate and its Cosmos persistence document
/// (docs/events/inventory.InternalHallmarkingStatusChanged.md §5.2).
/// </summary>
public class ItemStockIntransitMapperTests
{
    private static ItemStockIntransit CreateAggregate(DateTime? modifiedUtc = null)
    {
        var aggregate = ItemStockIntransit.CreateDefault(
            "SKU-1", "925", "TH", "INTERNALHALLMARKING", "WH-1", "ALLOCATED",
            modifiedUtc ?? new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
        aggregate.IncreaseQuantity(4, modifiedUtc ?? new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
        aggregate.ETag = "etag-1";

        return aggregate;
    }

    [Fact(DisplayName = "ToDocument copies every aggregate field onto the document, including the composite id as both Id and Category")]
    public void ToDocument_ValidAggregate_CopiesAllFieldsAndId()
    {
        var aggregate = CreateAggregate();

        var document = ItemStockIntransitMapper.ToDocument(aggregate);

        Assert.Equal(aggregate.Id, document.Id);
        Assert.Equal(aggregate.Category, document.Category);
        Assert.Equal("SKU-1", document.ItemCode);
        Assert.Equal("925", document.HallmarkCode);
        Assert.Equal("TH", document.CountryOfOriginCode);
        Assert.Equal("INTERNALHALLMARKING", document.OrderType);
        Assert.Equal("WH-1", document.FulfilmentCode);
        Assert.Equal("ALLOCATED", document.Status);
        Assert.Equal(4, document.Quantity);
        Assert.Equal("etag-1", document.ETag);
    }

    [Fact(DisplayName = "ToDocument serializes ModifiedUtc as a round-trippable ISO 8601 timestamp")]
    public void ToDocument_ValidAggregate_SerializesModifiedUtcAsRoundtripString()
    {
        var modifiedUtc = new DateTime(2026, 7, 8, 3, 45, 12, DateTimeKind.Utc);
        var aggregate = CreateAggregate(modifiedUtc);

        var document = ItemStockIntransitMapper.ToDocument(aggregate);

        Assert.Equal(modifiedUtc.ToString("O"), document.Timestamp);
    }

    [Fact(DisplayName = "ToDomain rehydrates every document field onto the aggregate and carries the ETag forward")]
    public void ToDomain_ValidDocument_RehydratesAllFieldsAndETag()
    {
        var document = new ItemStockIntransitDocument
        {
            Id = "SKU-1:925:TH:INTERNALHALLMARKING:WH-1:PICKED",
            Category = "SKU-1:925:TH:INTERNALHALLMARKING:WH-1:PICKED",
            ItemCode = "SKU-1",
            HallmarkCode = "925",
            CountryOfOriginCode = "TH",
            OrderType = "INTERNALHALLMARKING",
            FulfilmentCode = "WH-1",
            Status = "PICKED",
            Quantity = 7,
            Timestamp = "2026-07-08T03:45:12.0000000Z",
            ETag = "etag-2",
        };

        var aggregate = ItemStockIntransitMapper.ToDomain(document);

        Assert.Equal(document.Id, aggregate.Id);
        Assert.Equal("SKU-1", aggregate.ItemCode);
        Assert.Equal("925", aggregate.HallmarkCode);
        Assert.Equal("TH", aggregate.CountryOfOriginCode);
        Assert.Equal("INTERNALHALLMARKING", aggregate.OrderType);
        Assert.Equal("WH-1", aggregate.FulfilmentCode);
        Assert.Equal("PICKED", aggregate.Status);
        Assert.Equal(7, aggregate.Quantity);
        Assert.Equal(new DateTime(2026, 7, 8, 3, 45, 12, DateTimeKind.Utc), aggregate.ModifiedUtc);
        Assert.Equal("etag-2", aggregate.ETag);
    }

    [Fact(DisplayName = "ToDomain defaults Quantity to zero when the document's Quantity is null")]
    public void ToDomain_NullQuantity_DefaultsToZero()
    {
        var document = new ItemStockIntransitDocument
        {
            Id = "SKU-1:925:TH:INTERNALHALLMARKING:WH-1:ALLOCATED",
            Category = "SKU-1:925:TH:INTERNALHALLMARKING:WH-1:ALLOCATED",
            ItemCode = "SKU-1",
            HallmarkCode = "925",
            CountryOfOriginCode = "TH",
            OrderType = "INTERNALHALLMARKING",
            FulfilmentCode = "WH-1",
            Status = "ALLOCATED",
            Quantity = null,
            Timestamp = "2026-07-08T03:45:12.0000000Z",
        };

        var aggregate = ItemStockIntransitMapper.ToDomain(document);

        Assert.Equal(0, aggregate.Quantity);
    }

    [Fact(DisplayName = "ToDomain falls back to the current UTC time when the document's Timestamp is unparseable")]
    public void ToDomain_UnparseableTimestamp_FallsBackToUtcNow()
    {
        var document = new ItemStockIntransitDocument
        {
            Id = "SKU-1:925:TH:INTERNALHALLMARKING:WH-1:ALLOCATED",
            Category = "SKU-1:925:TH:INTERNALHALLMARKING:WH-1:ALLOCATED",
            ItemCode = "SKU-1",
            HallmarkCode = "925",
            CountryOfOriginCode = "TH",
            OrderType = "INTERNALHALLMARKING",
            FulfilmentCode = "WH-1",
            Status = "ALLOCATED",
            Quantity = 1,
            Timestamp = "not-a-date",
        };

        var before = DateTime.UtcNow;
        var aggregate = ItemStockIntransitMapper.ToDomain(document);
        var after = DateTime.UtcNow;

        Assert.InRange(aggregate.ModifiedUtc, before, after);
    }
}
