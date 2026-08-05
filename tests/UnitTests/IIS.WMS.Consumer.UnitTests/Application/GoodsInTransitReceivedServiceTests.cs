using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>
/// Tests for <see cref="GoodsInTransitReceivedService"/> - sellable (§3.2/§6.1) vs. non-sellable
/// (§3.7/§6.2) receipt paths, the create-vs-patch branch, concurrency retry/exhaustion, and the
/// non-sellable path's "ensure zeroed main record exists" side effect
/// (docs/events/b2b.purchase.GoodsInTransitReceived.md).
/// </summary>
public class GoodsInTransitReceivedServiceTests
{
    private static readonly DateTimeOffset Now = new(2026, 7, 8, 12, 0, 0, TimeSpan.Zero);
    private const string FulfilmentId = "CAECOM";
    private const string ItemCode = "SKU-1";
    private const string CountryOfOrigin = "DK";
    private const string Hallmark = "585";
    private static readonly string MainId = ItemStockInventory.BuildId(FulfilmentId, ItemCode, Hallmark, CountryOfOrigin);

    private readonly IItemStockInventoryRepository repository = Substitute.For<IItemStockInventoryRepository>();
    private readonly IItemStockInventoryExtendedRepository extendedRepository = Substitute.For<IItemStockInventoryExtendedRepository>();
    private readonly TimeProvider timeProvider = Substitute.For<TimeProvider>();
    private readonly GoodsInTransitReceivedService sut;

    public GoodsInTransitReceivedServiceTests()
    {
        timeProvider.GetUtcNow().Returns(Now);
        sut = new GoodsInTransitReceivedService(
            repository, extendedRepository, timeProvider, Substitute.For<ILogger<GoodsInTransitReceivedService>>());
    }

    private static ItemStockInventory CreateMainAggregate(string etag)
    {
        var aggregate = ItemStockInventory.CreateDefault(FulfilmentId, ItemCode, Hallmark, CountryOfOrigin, Now.UtcDateTime);
        aggregate.ETag = etag;
        return aggregate;
    }

    [Fact(DisplayName = "ReceiveShipmentLineAsync sellable path creates a new record and reports the delta when none exists")]
    public async Task ReceiveShipmentLineAsync_SellableNoExistingRecord_CreatesRecordAndReportsDelta()
    {
        repository.GetAsync(MainId, MainId, Arg.Any<CancellationToken>()).Returns((ItemStockInventory?)null);

        var result = await sut.ReceiveShipmentLineAsync(
            FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 5, isSellable: true, State.AVAILABLE, Status.HELD, CancellationToken.None);

        await repository.Received(1).CreateAsync(
            Arg.Is<ItemStockInventory>(a => a.B2CAvailable == 5), Arg.Any<CancellationToken>());
        await repository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
        Assert.True(result.IsB2CChanged);
        Assert.Equal(5, result.DeltaTowardsOms);
    }

    [Fact(DisplayName = "ReceiveShipmentLineAsync sellable path increments B2CAVL via Patch when a record already exists")]
    public async Task ReceiveShipmentLineAsync_SellableExistingRecord_PatchesIncrement()
    {
        var aggregate = CreateMainAggregate("etag-1");
        repository.GetAsync(MainId, MainId, Arg.Any<CancellationToken>()).Returns(aggregate);
        repository.PatchAsync(MainId, MainId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);

        var result = await sut.ReceiveShipmentLineAsync(
            FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 7, isSellable: true, State.AVAILABLE, Status.HELD, CancellationToken.None);

        await repository.Received(1).PatchAsync(
            MainId, MainId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>());
        await repository.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
        Assert.True(result.IsB2CChanged);
        Assert.Equal(7, result.DeltaTowardsOms);
    }

    [Fact(DisplayName = "ReceiveShipmentLineAsync sellable path retries against fresh state after a concurrency conflict")]
    public async Task ReceiveShipmentLineAsync_SellableConcurrencyConflictOnFirstAttempt_RetriesAndSucceeds()
    {
        var staleAggregate = CreateMainAggregate("stale-etag");
        var freshAggregate = CreateMainAggregate("fresh-etag");
        repository.GetAsync(MainId, MainId, Arg.Any<CancellationToken>()).Returns(staleAggregate, freshAggregate);
        repository.PatchAsync(MainId, MainId, "stale-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(MainId, "stale-etag"));
        repository.PatchAsync(MainId, MainId, "fresh-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(freshAggregate);

        var result = await sut.ReceiveShipmentLineAsync(
            FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 3, isSellable: true, State.AVAILABLE, Status.HELD, CancellationToken.None);

        await repository.Received(2).GetAsync(MainId, MainId, Arg.Any<CancellationToken>());
        Assert.Equal(3, result.DeltaTowardsOms);
    }

    [Fact(DisplayName = "ReceiveShipmentLineAsync sellable path throws ConcurrencyException once retries are exhausted")]
    public async Task ReceiveShipmentLineAsync_SellableConcurrencyConflictOnEveryAttempt_ThrowsAfterExhaustingRetries()
    {
        repository.GetAsync(MainId, MainId, Arg.Any<CancellationToken>())
            .Returns(_ => CreateMainAggregate("etag-x"));
        repository.PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(MainId, "etag-x"));

        await Assert.ThrowsAsync<ConcurrencyException>(
            () => sut.ReceiveShipmentLineAsync(
                FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 3, isSellable: true, State.AVAILABLE, Status.HELD, CancellationToken.None));

        await repository.Received(3).GetAsync(MainId, MainId, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ReceiveShipmentLineAsync non-sellable path creates the extended record when none exists and ensures a zeroed main record")]
    public async Task ReceiveShipmentLineAsync_NonSellableNoExistingRecord_CreatesExtendedAndEnsuresMainRecord()
    {
        repository.GetAsync(MainId, MainId, Arg.Any<CancellationToken>()).Returns((ItemStockInventory?)null);
        extendedRepository.GetAsync(FulfilmentId, ItemCode, Hallmark, CountryOfOrigin, State.INSPECTION, Status.HELD, Arg.Any<CancellationToken>())
            .Returns((ItemStockInventoryExtended?)null);

        var result = await sut.ReceiveShipmentLineAsync(
            FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 4, isSellable: false, State.INSPECTION, Status.HELD, CancellationToken.None);

        await repository.Received(1).CreateAsync(
            Arg.Is<ItemStockInventory>(a => a.B2CAvailable == 0), Arg.Any<CancellationToken>());
        await extendedRepository.Received(1).CreateAsync(
            Arg.Is<ItemStockInventoryExtended>(e => e.Qty == 4 && e.State == State.INSPECTION && e.Status == Status.HELD),
            Arg.Any<CancellationToken>());
        Assert.False(result.IsB2CChanged);
        Assert.Equal(0, result.DeltaTowardsOms);
    }

    [Fact(DisplayName = "ReceiveShipmentLineAsync non-sellable path does not recreate the main record when one already exists")]
    public async Task ReceiveShipmentLineAsync_NonSellableExistingMainRecord_DoesNotRecreateMainRecord()
    {
        repository.GetAsync(MainId, MainId, Arg.Any<CancellationToken>()).Returns(CreateMainAggregate("etag-1"));
        extendedRepository.GetAsync(FulfilmentId, ItemCode, Hallmark, CountryOfOrigin, State.INSPECTION, Status.HELD, Arg.Any<CancellationToken>())
            .Returns((ItemStockInventoryExtended?)null);

        await sut.ReceiveShipmentLineAsync(
            FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 4, isSellable: false, State.INSPECTION, Status.HELD, CancellationToken.None);

        await repository.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
    }

    [Fact(DisplayName = "ReceiveShipmentLineAsync non-sellable path increments Qty via Patch when the extended record already exists")]
    public async Task ReceiveShipmentLineAsync_NonSellableExistingExtendedRecord_PatchesIncrement()
    {
        var extendedId = ItemStockInventoryExtended.BuildId(FulfilmentId, ItemCode, Hallmark, CountryOfOrigin, State.INSPECTION, Status.HELD);
        var entity = new ItemStockInventoryExtended
        {
            FulfilmentId = FulfilmentId, ItemCode = ItemCode, COO = CountryOfOrigin, Hallmark = Hallmark,
            State = State.INSPECTION, Status = Status.HELD, Qty = 10, ETag = "ext-etag-1",
        };
        repository.GetAsync(MainId, MainId, Arg.Any<CancellationToken>()).Returns(CreateMainAggregate("etag-1"));
        extendedRepository.GetAsync(FulfilmentId, ItemCode, Hallmark, CountryOfOrigin, State.INSPECTION, Status.HELD, Arg.Any<CancellationToken>())
            .Returns(entity);
        extendedRepository.PatchAsync(extendedId, extendedId, "ext-etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(entity);

        var result = await sut.ReceiveShipmentLineAsync(
            FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 6, isSellable: false, State.INSPECTION, Status.HELD, CancellationToken.None);

        await extendedRepository.Received(1).PatchAsync(
            extendedId, extendedId, "ext-etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>());
        await extendedRepository.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
        Assert.False(result.IsB2CChanged);
        Assert.Equal(0, result.DeltaTowardsOms);
    }

    [Fact(DisplayName = "ReceiveShipmentLineAsync non-sellable path retries against fresh state after a concurrency conflict")]
    public async Task ReceiveShipmentLineAsync_NonSellableConcurrencyConflictOnFirstAttempt_RetriesAndSucceeds()
    {
        var extendedId = ItemStockInventoryExtended.BuildId(FulfilmentId, ItemCode, Hallmark, CountryOfOrigin, State.INSPECTION, Status.HELD);
        var staleEntity = new ItemStockInventoryExtended
        {
            FulfilmentId = FulfilmentId, ItemCode = ItemCode, COO = CountryOfOrigin, Hallmark = Hallmark,
            State = State.INSPECTION, Status = Status.HELD, Qty = 10, ETag = "stale-etag",
        };
        var freshEntity = new ItemStockInventoryExtended
        {
            FulfilmentId = FulfilmentId, ItemCode = ItemCode, COO = CountryOfOrigin, Hallmark = Hallmark,
            State = State.INSPECTION, Status = Status.HELD, Qty = 12, ETag = "fresh-etag",
        };
        repository.GetAsync(MainId, MainId, Arg.Any<CancellationToken>()).Returns(CreateMainAggregate("etag-1"));
        extendedRepository.GetAsync(FulfilmentId, ItemCode, Hallmark, CountryOfOrigin, State.INSPECTION, Status.HELD, Arg.Any<CancellationToken>())
            .Returns(staleEntity, freshEntity);
        extendedRepository.PatchAsync(extendedId, extendedId, "stale-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(extendedId, "stale-etag"));
        extendedRepository.PatchAsync(extendedId, extendedId, "fresh-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(freshEntity);

        await sut.ReceiveShipmentLineAsync(
            FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 2, isSellable: false, State.INSPECTION, Status.HELD, CancellationToken.None);

        await extendedRepository.Received(2).GetAsync(
            FulfilmentId, ItemCode, Hallmark, CountryOfOrigin, State.INSPECTION, Status.HELD, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ReceiveShipmentLineAsync non-sellable path throws ConcurrencyException once retries are exhausted")]
    public async Task ReceiveShipmentLineAsync_NonSellableConcurrencyConflictOnEveryAttempt_ThrowsAfterExhaustingRetries()
    {
        repository.GetAsync(MainId, MainId, Arg.Any<CancellationToken>()).Returns(CreateMainAggregate("etag-1"));
        extendedRepository.GetAsync(
            FulfilmentId, ItemCode, Hallmark, CountryOfOrigin, State.INSPECTION, Status.HELD, Arg.Any<CancellationToken>())
            .Returns(_ => new ItemStockInventoryExtended
            {
                FulfilmentId = FulfilmentId, ItemCode = ItemCode, COO = CountryOfOrigin, Hallmark = Hallmark,
                State = State.INSPECTION, Status = Status.HELD, Qty = 10, ETag = "etag-x",
            });
        extendedRepository.PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException("ext-id", "etag-x"));

        await Assert.ThrowsAsync<ConcurrencyException>(
            () => sut.ReceiveShipmentLineAsync(
                FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 2, isSellable: false, State.INSPECTION, Status.HELD, CancellationToken.None));

        await extendedRepository.Received(3).GetAsync(
            FulfilmentId, ItemCode, Hallmark, CountryOfOrigin, State.INSPECTION, Status.HELD, Arg.Any<CancellationToken>());
    }
}
