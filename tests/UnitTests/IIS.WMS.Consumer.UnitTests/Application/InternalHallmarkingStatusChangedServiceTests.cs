using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InternalHallmarkingStatusChanged;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>
/// Use-case orchestration tests for <see cref="InternalHallmarkingStatusChangedService"/>'s four
/// status-path methods (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.1-§3.5), with the
/// inventory/intransit repositories, segmentation service, and domain-event dispatcher mocked.
/// </summary>
public class InternalHallmarkingStatusChangedServiceTests
{
    private static readonly DateTimeOffset Now = new(2026, 7, 8, 12, 0, 0, TimeSpan.Zero);
    private const string FulfilmentId = "WH1";
    private const string ItemCode = "SKU1";
    private const string CountryOfOrigin = "TH";
    private const string Hallmark = "925";
    private const string InventoryId = "WH1:SKU1:925:TH";

    private readonly IItemStockInventoryRepository inventoryRepository = Substitute.For<IItemStockInventoryRepository>();
    private readonly IItemStockIntransitRepository intransitRepository = Substitute.For<IItemStockIntransitRepository>();
    private readonly IItemStockInventorySegmentationService segmentationService = Substitute.For<IItemStockInventorySegmentationService>();
    private readonly IDomainEventDispatcher domainEventDispatcher = Substitute.For<IDomainEventDispatcher>();
    private readonly TimeProvider timeProvider = Substitute.For<TimeProvider>();
    private readonly InternalHallmarkingStatusChangedService sut;

    public InternalHallmarkingStatusChangedServiceTests()
    {
        timeProvider.GetUtcNow().Returns(Now);
        sut = new InternalHallmarkingStatusChangedService(
            inventoryRepository, intransitRepository, segmentationService, domainEventDispatcher, timeProvider,
            Substitute.For<ILogger<InternalHallmarkingStatusChangedService>>());

        intransitRepository.GetAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns((ItemStockIntransit?)null);
        intransitRepository.CreateAsync(Arg.Any<ItemStockIntransit>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(callInfo.Arg<ItemStockIntransit>()));
    }

    private static ItemStockInventory CreateAggregate(
        string etag, int b2bAllocated = 10, int b2bAvailable = 20, int inTransit = 0, bool isExtended = false) =>
        ItemStockInventory.Rehydrate(
            InventoryId, FulfilmentId, ItemCode, CountryOfOrigin, Hallmark,
            b2bAvailable: b2bAvailable, b2cAvailable: 20, b2cOriginal: 20, b2cExtended: 0,
            b2cAllocated: 5, b2bAllocated: b2bAllocated, b2cPrepared: 0, b2bPrepared: 0,
            internalHallmarkAllocated: 0, inTransit: inTransit, b2cThreshold: 0, isExtended: isExtended, b2bUsedShare: 0,
            inspection: 0, psc: 0, isPosm: false, modifiedUtc: Now.UtcDateTime) is var aggregate
            ? SetEtag(aggregate, etag)
            : throw new InvalidOperationException();

    private static ItemStockInventory SetEtag(ItemStockInventory aggregate, string etag)
    {
        aggregate.ETag = etag;
        return aggregate;
    }

    /// <summary>
    /// Seeds an existing in-transit leg with enough quantity to survive a decrement, so
    /// <c>UpsertIntransitLegAsync</c> routes through <c>PatchAsync</c> (existing leg) rather than
    /// silently failing <see cref="ItemStockIntransit.DecreaseQuantity"/>'s never-negative invariant
    /// against a freshly-created (zero-quantity) leg.
    /// </summary>
    private void StubExistingIntransitLeg(string hallmarkCode, string status, int quantity, string etag)
    {
        var id = ItemStockIntransit.BuildId(ItemCode, hallmarkCode, CountryOfOrigin, "INTERNALHALLMARKING", FulfilmentId, status);
        var entity = ItemStockIntransit.Rehydrate(
            id, ItemCode, hallmarkCode, CountryOfOrigin, "INTERNALHALLMARKING", FulfilmentId, status, quantity, Now.UtcDateTime);
        entity.ETag = etag;

        intransitRepository.GetAsync(id, id, Arg.Any<CancellationToken>()).Returns(entity);
        intransitRepository.PatchAsync(id, id, etag, Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(entity);
    }

    // ---------- AllocateAsync (§3.1 STARTED) ----------

    [Fact(DisplayName = "AllocateAsync returns a no-change result without throwing when no ItemStockInventory record exists")]
    public async Task AllocateAsync_MissingInventory_ReturnsNoChangeWithoutThrowing()
    {
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns((ItemStockInventory?)null);

        var result = await sut.AllocateAsync(FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 4, CancellationToken.None);

        Assert.False(result.IsB2CChanged);
        Assert.Equal(0, result.DeltaTowardsOms);
        await inventoryRepository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }

    [Fact(DisplayName = "AllocateAsync returns a no-change result without throwing when the aggregate rejects the quantity")]
    public async Task AllocateAsync_AggregateRejectsQuantity_ReturnsNoChangeWithoutThrowing()
    {
        var aggregate = CreateAggregate("etag-1", b2bAllocated: 5);
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns(aggregate);

        var result = await sut.AllocateAsync(FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, -10, CancellationToken.None);

        Assert.False(result.IsB2CChanged);
        await inventoryRepository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }

    [Fact(DisplayName = "AllocateAsync patches B2BAllocated, dispatches domain events, and creates the ALLOCATED transit leg")]
    public async Task AllocateAsync_ValidAllocation_PatchesAggregateDispatchesEventsAndCreatesAllocatedLeg()
    {
        var aggregate = CreateAggregate("etag-1", b2bAllocated: 10);
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns(aggregate);
        inventoryRepository.PatchAsync(InventoryId, InventoryId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);

        var result = await sut.AllocateAsync(FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 4, CancellationToken.None);

        Assert.Equal(14, aggregate.B2BAllocated);
        Assert.False(result.IsB2CChanged);
        await inventoryRepository.Received(1).PatchAsync(
            InventoryId, InventoryId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>());
        await intransitRepository.Received(1).CreateAsync(
            Arg.Is<ItemStockIntransit>(e => e.Status == "ALLOCATED" && e.Quantity == 4), Arg.Any<CancellationToken>());
        await domainEventDispatcher.Received(2).DispatchAsync(Arg.Any<IReadOnlyCollection<IDomainEvent>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "AllocateAsync retries against fresh state after a concurrency conflict")]
    public async Task AllocateAsync_ConcurrencyConflictOnFirstAttempt_RetriesAndSucceeds()
    {
        var staleAggregate = CreateAggregate("stale-etag", b2bAllocated: 10);
        var freshAggregate = CreateAggregate("fresh-etag", b2bAllocated: 10);
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns(staleAggregate, freshAggregate);
        inventoryRepository.PatchAsync(InventoryId, InventoryId, "stale-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(InventoryId, "stale-etag"));
        inventoryRepository.PatchAsync(InventoryId, InventoryId, "fresh-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(freshAggregate);

        await sut.AllocateAsync(FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 4, CancellationToken.None);

        await inventoryRepository.Received(2).GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>());
        Assert.Equal(14, freshAggregate.B2BAllocated);
    }

    [Fact(DisplayName = "AllocateAsync throws ConcurrencyException once retries are exhausted")]
    public async Task AllocateAsync_ConcurrencyConflictOnEveryAttempt_ThrowsAfterExhaustingRetries()
    {
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>())
            .Returns(_ => CreateAggregate("etag-x", b2bAllocated: 10));
        inventoryRepository.PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(InventoryId, "etag-x"));

        await Assert.ThrowsAsync<ConcurrencyException>(
            () => sut.AllocateAsync(FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 4, CancellationToken.None));

        await inventoryRepository.Received(3).GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>());
    }

    // ---------- PickAndShipAsync (§3.2/§3.3 PICKED) ----------

    [Fact(DisplayName = "PickAndShipAsync patches the aggregate, dispatches domain events, and upserts both the PICKED and ALLOCATED transit legs")]
    public async Task PickAndShipAsync_ValidPick_PatchesAggregateAndUpsertsBothTransitLegs()
    {
        var aggregate = CreateAggregate("etag-1", b2bAllocated: 10, b2bAvailable: 20);
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns(aggregate);
        inventoryRepository.PatchAsync(InventoryId, InventoryId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);
        StubExistingIntransitLeg(Hallmark, "ALLOCATED", quantity: 10, etag: "leg-etag-1");

        await sut.PickAndShipAsync(FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 4, CancellationToken.None);

        Assert.Equal(6, aggregate.B2BAllocated);
        await inventoryRepository.Received(1).PatchAsync(
            InventoryId, InventoryId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>());
        await intransitRepository.Received(1).CreateAsync(
            Arg.Is<ItemStockIntransit>(e => e.Status == "PICKED" && e.Quantity == 4), Arg.Any<CancellationToken>());
        await intransitRepository.Received(1).PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), "leg-etag-1",
            Arg.Is<IReadOnlyList<PatchOperation>>(ops => ops.Any(op => op.OperationType == PatchOperationType.Increment)),
            Arg.Any<CancellationToken>());
    }

    // ---------- ChangeHallmarkAsync (§3.4 CHANGED) ----------

    [Fact(DisplayName = "ChangeHallmarkAsync combines the to/from segmentation deltas and upserts both transit legs")]
    public async Task ChangeHallmarkAsync_ValidChange_CombinesDeltasAndUpsertsBothTransitLegs()
    {
        const string hallmarkTo = "950";
        const string hallmarkFrom = "925";
        segmentationService.ApplySegmentationAsync(
                FulfilmentId, ItemCode, CountryOfOrigin, hallmarkTo, 4, false, Arg.Any<CancellationToken>())
            .Returns(new ItemStockInventoryDeltaResult { IsB2CChanged = true, DeltaTowardsOms = 4 });
        segmentationService.ApplySegmentationAsync(
                FulfilmentId, ItemCode, CountryOfOrigin, hallmarkFrom, -4, false, Arg.Any<CancellationToken>())
            .Returns(new ItemStockInventoryDeltaResult { IsB2CChanged = false, DeltaTowardsOms = -4 });
        StubExistingIntransitLeg(hallmarkFrom, "INTRANSIT", quantity: 10, etag: "leg-etag-1");

        var result = await sut.ChangeHallmarkAsync(
            FulfilmentId, ItemCode, CountryOfOrigin, hallmarkFrom, hallmarkTo, 4, isThirdPartyLogistics: false, CancellationToken.None);

        Assert.True(result.IsB2CChanged);
        Assert.Equal(0, result.DeltaTowardsOms);
        await intransitRepository.Received(1).CreateAsync(
            Arg.Is<ItemStockIntransit>(e => e.HallmarkCode == hallmarkTo && e.Status == "INTRANSIT" && e.Quantity == 4),
            Arg.Any<CancellationToken>());
        await intransitRepository.Received(1).PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), "leg-etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>());
    }

    // ---------- CompleteTransitAsync (§3.5 FINISHED) ----------

    [Fact(DisplayName = "CompleteTransitAsync returns without throwing when no ItemStockInventory record exists")]
    public async Task CompleteTransitAsync_MissingInventory_ReturnsWithoutThrowing()
    {
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns((ItemStockInventory?)null);

        await sut.CompleteTransitAsync(FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 4, CancellationToken.None);

        await inventoryRepository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }

    [Fact(DisplayName = "CompleteTransitAsync returns without throwing when the aggregate rejects the quantity")]
    public async Task CompleteTransitAsync_AggregateRejectsQuantity_ReturnsWithoutThrowing()
    {
        var aggregate = CreateAggregate("etag-1", inTransit: 2);
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns(aggregate);

        await sut.CompleteTransitAsync(FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 5, CancellationToken.None);

        await inventoryRepository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }

    [Fact(DisplayName = "CompleteTransitAsync patches the aggregate and upserts both the INTRANSIT (decrement) and CREATED (increment) transit legs")]
    public async Task CompleteTransitAsync_ValidCompletion_PatchesAggregateAndUpsertsBothTransitLegs()
    {
        var aggregate = CreateAggregate("etag-1", b2bAvailable: 20, inTransit: 10);
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns(aggregate);
        inventoryRepository.PatchAsync(InventoryId, InventoryId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);
        StubExistingIntransitLeg(Hallmark, "INTRANSIT", quantity: 10, etag: "leg-etag-1");

        await sut.CompleteTransitAsync(FulfilmentId, ItemCode, CountryOfOrigin, Hallmark, 4, CancellationToken.None);

        Assert.Equal(6, aggregate.InTransit);
        Assert.Equal(24, aggregate.B2BAvailable);
        await intransitRepository.Received(1).PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), "leg-etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>());
        await intransitRepository.Received(1).CreateAsync(
            Arg.Is<ItemStockIntransit>(e => e.Status == "CREATED" && e.Quantity == 4), Arg.Any<CancellationToken>());
    }
}
