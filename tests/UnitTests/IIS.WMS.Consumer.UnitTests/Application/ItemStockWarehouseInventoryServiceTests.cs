using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>
/// Use-case orchestration tests for <see cref="ItemStockWarehouseInventoryService.ApplyShipmentAsync"/>
/// (docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.3 step 3), with the warehouse-inventory
/// repository mocked.
/// </summary>
public class ItemStockWarehouseInventoryServiceTests
{
    private static readonly DateTimeOffset Now = new(2026, 7, 8, 12, 0, 0, TimeSpan.Zero);
    private const string FulfilmentId = "DEECOMDC";
    private const string ItemCode = "SKU1";
    private const string RecordId = "DEECOMDC:SKU1";

    private readonly IItemStockWarehouseInventoryRepository repository = Substitute.For<IItemStockWarehouseInventoryRepository>();
    private readonly TimeProvider timeProvider = Substitute.For<TimeProvider>();
    private readonly ItemStockWarehouseInventoryService sut;

    public ItemStockWarehouseInventoryServiceTests()
    {
        timeProvider.GetUtcNow().Returns(Now);
        sut = new ItemStockWarehouseInventoryService(
            repository, timeProvider, Substitute.For<ILogger<ItemStockWarehouseInventoryService>>());
    }

    [Theory(DisplayName = "ApplyShipmentAsync rejects a non-positive quantity")]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ApplyShipmentAsync_NonPositiveQuantity_ThrowsArgumentOutOfRangeException(int quantity)
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => sut.ApplyShipmentAsync(FulfilmentId, ItemCode, quantity, CancellationToken.None));

        await repository.DidNotReceiveWithAnyArgs().GetAsync(default!, default!, default);
    }

    [Fact(DisplayName = "ApplyShipmentAsync creates a new record seeded with the shipped quantity when none exists")]
    public async Task ApplyShipmentAsync_MissingRecord_CreatesNewRecordWithShippedQuantity()
    {
        repository.GetAsync(RecordId, RecordId, Arg.Any<CancellationToken>()).Returns((ItemStockWarehouseInventory?)null);
        repository.CreateAsync(Arg.Any<ItemStockWarehouseInventory>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(callInfo.Arg<ItemStockWarehouseInventory>()));

        await sut.ApplyShipmentAsync(FulfilmentId, ItemCode, 5, CancellationToken.None);

        await repository.Received(1).CreateAsync(
            Arg.Is<ItemStockWarehouseInventory>(e => e.Id == RecordId && e.Qnty == 5), Arg.Any<CancellationToken>());
        await repository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }

    [Fact(DisplayName = "ApplyShipmentAsync increments the existing record's quantity via Patch when found")]
    public async Task ApplyShipmentAsync_ExistingRecord_PatchesIncrementedQuantity()
    {
        var existing = ItemStockWarehouseInventory.Rehydrate(RecordId, FulfilmentId, ItemCode, 10, Now.UtcDateTime);
        existing.ETag = "etag-1";
        repository.GetAsync(RecordId, RecordId, Arg.Any<CancellationToken>()).Returns(existing);
        repository.PatchAsync(RecordId, RecordId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(existing);

        await sut.ApplyShipmentAsync(FulfilmentId, ItemCode, 5, CancellationToken.None);

        await repository.Received(1).PatchAsync(
            RecordId, RecordId, "etag-1",
            Arg.Is<IReadOnlyList<PatchOperation>>(ops => ops.Any(op => op.OperationType == PatchOperationType.Increment)),
            Arg.Any<CancellationToken>());
        await repository.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
    }

    [Fact(DisplayName = "ApplyShipmentAsync retries against fresh state after a concurrency conflict")]
    public async Task ApplyShipmentAsync_ConcurrencyConflictOnFirstAttempt_RetriesAndSucceeds()
    {
        var staleExisting = ItemStockWarehouseInventory.Rehydrate(RecordId, FulfilmentId, ItemCode, 10, Now.UtcDateTime);
        staleExisting.ETag = "stale-etag";
        var freshExisting = ItemStockWarehouseInventory.Rehydrate(RecordId, FulfilmentId, ItemCode, 10, Now.UtcDateTime);
        freshExisting.ETag = "fresh-etag";
        repository.GetAsync(RecordId, RecordId, Arg.Any<CancellationToken>()).Returns(staleExisting, freshExisting);
        repository.PatchAsync(RecordId, RecordId, "stale-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(RecordId, "stale-etag"));
        repository.PatchAsync(RecordId, RecordId, "fresh-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(freshExisting);

        await sut.ApplyShipmentAsync(FulfilmentId, ItemCode, 5, CancellationToken.None);

        await repository.Received(2).GetAsync(RecordId, RecordId, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplyShipmentAsync throws ConcurrencyException once retries are exhausted")]
    public async Task ApplyShipmentAsync_ConcurrencyConflictOnEveryAttempt_ThrowsAfterExhaustingRetries()
    {
        repository.GetAsync(RecordId, RecordId, Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var existing = ItemStockWarehouseInventory.Rehydrate(RecordId, FulfilmentId, ItemCode, 10, Now.UtcDateTime);
                existing.ETag = "etag-x";
                return existing;
            });
        repository.PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(RecordId, "etag-x"));

        await Assert.ThrowsAsync<ConcurrencyException>(
            () => sut.ApplyShipmentAsync(FulfilmentId, ItemCode, 5, CancellationToken.None));

        await repository.Received(3).GetAsync(RecordId, RecordId, Arg.Any<CancellationToken>());
    }
}
