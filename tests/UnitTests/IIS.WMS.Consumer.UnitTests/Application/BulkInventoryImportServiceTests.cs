using IIS.WMS.Consumer.Application.BulkInventoryImport;
using IIS.WMS.Consumer.Application.BulkInventoryImport.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Use-case orchestration tests for <see cref="BulkInventoryImportService"/>, with the repository mocked.</summary>
public class BulkInventoryImportServiceTests
{
    private static readonly DateTime Now = new(2026, 7, 16, 12, 0, 0, DateTimeKind.Utc);

    private readonly IBulkInventoryImportRepository repository = Substitute.For<IBulkInventoryImportRepository>();
    private readonly BulkInventoryImportService sut;

    public BulkInventoryImportServiceTests()
    {
        sut = new BulkInventoryImportService(repository, Substitute.For<ILogger<BulkInventoryImportService>>());
    }

    [Fact(DisplayName = "ImportAsync maps the request to an aggregate and upserts it")]
    public async Task ImportAsync_ValidRequest_UpsertsMappedAggregate()
    {
        var request = new ImportBulkInventoryItemRequest("WH1", "SKU1", 25, "WMS-Legacy", Now);
        InventoryBulkImportItem? persisted = null;
        repository.UpsertAsync(Arg.Do<InventoryBulkImportItem>(item => persisted = item), Arg.Any<CancellationToken>())
            .Returns(callInfo => callInfo.Arg<InventoryBulkImportItem>());

        await sut.ImportAsync(request);

        Assert.NotNull(persisted);
        Assert.Equal("WH1:SKU1", persisted!.Id);
        Assert.Equal("WH1", persisted.WarehouseId);
        Assert.Equal("SKU1", persisted.Sku);
        Assert.Equal(25, persisted.Quantity);
        Assert.Equal("WMS-Legacy", persisted.SourceSystem);
        Assert.Equal(Now, persisted.SourceLastUpdatedUtc);
    }

    [Fact(DisplayName = "ImportAsync passes the provided cancellation token through to the repository")]
    public async Task ImportAsync_ProvidedCancellationToken_PassesTokenToRepository()
    {
        using var cts = new CancellationTokenSource();
        var request = new ImportBulkInventoryItemRequest("WH1", "SKU1", 25, "WMS-Legacy", Now);
        repository.UpsertAsync(Arg.Any<InventoryBulkImportItem>(), cts.Token)
            .Returns(callInfo => callInfo.Arg<InventoryBulkImportItem>());

        await sut.ImportAsync(request, cts.Token);

        await repository.Received(1).UpsertAsync(Arg.Any<InventoryBulkImportItem>(), cts.Token);
    }

    [Fact(DisplayName = "ImportAsync propagates the exception when the aggregate cannot be created from an invalid request")]
    public async Task ImportAsync_InvalidWarehouseId_PropagatesArgumentException()
    {
        var request = new ImportBulkInventoryItemRequest(string.Empty, "SKU1", 25, "WMS-Legacy", Now);

        await Assert.ThrowsAnyAsync<ArgumentException>(() => sut.ImportAsync(request));

        await repository.DidNotReceive().UpsertAsync(Arg.Any<InventoryBulkImportItem>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ImportAsync propagates the exception when the repository upsert fails")]
    public async Task ImportAsync_RepositoryThrows_PropagatesException()
    {
        var request = new ImportBulkInventoryItemRequest("WH1", "SKU1", 25, "WMS-Legacy", Now);
        repository.UpsertAsync(Arg.Any<InventoryBulkImportItem>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("Cosmos is unavailable."));

        await Assert.ThrowsAsync<InvalidOperationException>(() => sut.ImportAsync(request));
    }
}
