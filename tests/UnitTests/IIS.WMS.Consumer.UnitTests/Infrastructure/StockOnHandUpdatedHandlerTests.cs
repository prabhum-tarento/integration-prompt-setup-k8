using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockOnHandUpdated;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockOnHandUpdated.Handlers;
using IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using DomainEnums = IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="StockOnHandUpdatedHandler"/> - Business Rule 1 (BRZ3PL-only), B2C/relevant-state
/// filtering and grouping, Business Rule 4 sellable/non-sellable classification, the discrepancy-gated
/// extended-state Increment (Calculation 2), the unconditional B2C publish, the ICR-snapshot feature-flag
/// gate, and the §8 per-path failure isolation with prioritized rethrow
/// (docs/events/inventory.StockOnHandUpdated.md).
/// </summary>
public class StockOnHandUpdatedHandlerTests
{
    private static StockOnHandQuantityDetail CreateDetail(
        int quantity = 5,
        InventoryEventStockState state = InventoryEventStockState.Available,
        InventoryEventStockStatus status = InventoryEventStockStatus.Pickable,
        string countryOfOrigin = "TH",
        string hallmarking = "NON",
        StockOnHandInventoryDomain domain = StockOnHandInventoryDomain.B2C) => new(
        Quantity: quantity,
        State: new InventoryEventStateSnapshot(state, status),
        CountryOfOrigin: countryOfOrigin,
        Hallmarking: hallmarking,
        Domain: domain);

    private static StockOnHandUpdatedEvent CreateEvent(
        string locationId = FulfilmentLocationIds.Brz3PlConsigneeId,
        string productId = "SKU-1",
        string referenceId = "REF-1",
        IReadOnlyList<StockOnHandQuantityDetail>? quantityDetails = null) => new(
        Channel: InventoryEventChannel.OwnOnline,
        ReferenceId: referenceId,
        UpdatedDate: new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
        Location: new InventoryEventLocation(locationId, InventoryEventLocationType.ThirdPartyLogistics),
        Entity: "ORG-1",
        ProductId: productId,
        ProductUnits: "EA",
        Barcode: "1234567890",
        QuantityDetails: quantityDetails ?? [CreateDetail()],
        Reason: StockOnHandUpdatedReason.Receipt);

    private static StockOnHandUpdatedHandler CreateHandler(
        out IItemStockInventoryService itemStockInventoryService,
        out IItemStockInventoryExtendedRepository extendedRepository,
        out IItemRepository itemRepository,
        out IStockOnHandUpdatedOmsPublisher omsPublisher,
        out IInventoryComparisonReportPublisher inventoryComparisonReportPublisher,
        FeatureFlagsOptions? featureFlags = null,
        ItemStockSyncApplyResult? applyResult = null)
    {
        itemStockInventoryService = Substitute.For<IItemStockInventoryService>();
        itemStockInventoryService.ApplyStockSyncAsync(
            default!, default!, default!, default!, default, default, default, default)
            .ReturnsForAnyArgs(applyResult ?? new ItemStockSyncApplyResult());

        extendedRepository = Substitute.For<IItemStockInventoryExtendedRepository>();
        extendedRepository.GetAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(), Arg.Any<CancellationToken>())
            .Returns((ItemStockInventoryExtended?)null);
        extendedRepository.CreateAsync(Arg.Any<ItemStockInventoryExtended>(), Arg.Any<CancellationToken>())
            .Returns(ci => ci.Arg<ItemStockInventoryExtended>());
        extendedRepository.PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(new ItemStockInventoryExtended());

        itemRepository = Substitute.For<IItemRepository>();
        itemRepository.GetByItemCodeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new Item { ItemCode = "SKU-1" });

        omsPublisher = Substitute.For<IStockOnHandUpdatedOmsPublisher>();
        inventoryComparisonReportPublisher = Substitute.For<IInventoryComparisonReportPublisher>();

        var archiveWriter = Substitute.For<IMessageArchiveWriter>();

        var featureFlagsOptions = Substitute.For<IOptions<FeatureFlagsOptions>>();
        featureFlagsOptions.Value.Returns(featureFlags ?? new FeatureFlagsOptions());

        var consumerOptions = Substitute.For<IOptions<StockOnHandUpdatedServiceBusConsumerOptions>>();
        consumerOptions.Value.Returns(new StockOnHandUpdatedServiceBusConsumerOptions());

        var timeProvider = Substitute.For<TimeProvider>();
        timeProvider.GetUtcNow().Returns(new DateTimeOffset(2026, 7, 1, 12, 0, 0, TimeSpan.Zero));

        return new StockOnHandUpdatedHandler(
            itemStockInventoryService,
            extendedRepository,
            itemRepository,
            omsPublisher,
            inventoryComparisonReportPublisher,
            archiveWriter,
            featureFlagsOptions,
            consumerOptions,
            timeProvider,
            Substitute.For<ILogger<StockOnHandUpdatedHandler>>());
    }

    [Fact(DisplayName = "HandleAsync Business Rule 1 skips all processing when the location is not the BRZ3PL consignee id")]
    public async Task HandleAsync_NonBrz3PlLocation_SkipsAllProcessing()
    {
        var target = CreateEvent(locationId: "WH-1");
        var sut = CreateHandler(out var itemStockInventoryService, out var extendedRepository, out var itemRepository, out var omsPublisher, out var icrPublisher);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemRepository.DidNotReceiveWithAnyArgs().GetByItemCodeAsync(default!, default);
        await itemStockInventoryService.DidNotReceiveWithAnyArgs().ApplyStockSyncAsync(default!, default!, default!, default!, default, default, default, default);
        await extendedRepository.DidNotReceiveWithAnyArgs().GetAsync(default!, default!, default!, default!, default, default, default);
        await omsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default!, default!, default!, default, default!, default);
        await icrPublisher.DidNotReceiveWithAnyArgs().PublishAsync(default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync assumption 5 auto-creates a missing item master record")]
    public async Task HandleAsync_MissingItemMaster_AutoCreatesItem()
    {
        var target = CreateEvent();
        var sut = CreateHandler(out _, out _, out var itemRepository, out _, out _);
        itemRepository.GetByItemCodeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns((Item?)null);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemRepository.Received(1).CreateAsync(
            Arg.Is<Item>(i => i.ItemCode == "SKU-1"), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync assumption 5 does not create the item master record when it already exists")]
    public async Task HandleAsync_ExistingItemMaster_DoesNotCreateItem()
    {
        var target = CreateEvent();
        var sut = CreateHandler(out _, out _, out var itemRepository, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemRepository.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
    }

    [Fact(DisplayName = "HandleAsync filters out non-B2C domain quantity details before grouping, skipping all downstream processing")]
    public async Task HandleAsync_NonB2CDomainDetail_ExcludedFromGrouping()
    {
        var target = CreateEvent(quantityDetails: [CreateDetail(domain: StockOnHandInventoryDomain.B2B)]);
        var sut = CreateHandler(out var itemStockInventoryService, out var extendedRepository, out _, out var omsPublisher, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryService.DidNotReceiveWithAnyArgs().ApplyStockSyncAsync(default!, default!, default!, default!, default, default, default, default);
        await extendedRepository.DidNotReceiveWithAnyArgs().GetAsync(default!, default!, default!, default!, default, default, default);
        await omsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default!, default!, default!, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync Business Rule 4 excludes a state/status pair that is neither sellable nor non-sellable from grouping")]
    public async Task HandleAsync_IrrelevantStateStatusPair_ExcludedFromGrouping()
    {
        var target = CreateEvent(quantityDetails:
            [CreateDetail(state: InventoryEventStockState.Blocked, status: InventoryEventStockStatus.Held)]);
        var sut = CreateHandler(out var itemStockInventoryService, out var extendedRepository, out _, out var omsPublisher, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryService.DidNotReceiveWithAnyArgs().ApplyStockSyncAsync(default!, default!, default!, default!, default, default, default, default);
        await extendedRepository.DidNotReceiveWithAnyArgs().GetAsync(default!, default!, default!, default!, default, default, default);
        await omsPublisher.DidNotReceiveWithAnyArgs().PublishAsync(
            default!, default!, default!, default!, default!, default!, default!, default!, default, default!, default);
    }

    [Fact(DisplayName = "HandleAsync Business Rule 4/6 sellable path calls ApplyStockSyncAsync with the Set (not incremented) b2cPrepared/b2cAvailableToSell/b2cAvl quantities")]
    public async Task HandleAsync_SellableStates_CallsApplyStockSyncWithComputedQuantities()
    {
        var target = CreateEvent(quantityDetails:
        [
            CreateDetail(quantity: 3, state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Prepared),
            CreateDetail(quantity: 4, state: InventoryEventStockState.AvailableToSell, status: InventoryEventStockStatus.Pickable),
        ]);
        var sut = CreateHandler(out var itemStockInventoryService, out _, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryService.Received(1).ApplyStockSyncAsync(
            FulfilmentLocationIds.BrzDc3PlFulfilmentId, "SKU-1", "TH", "NON", 7, 3, 4, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync Business Rule 5 normalizes a negative sellable quantity to 0 before the Set")]
    public async Task HandleAsync_NegativeSellableQuantity_NormalizesToZero()
    {
        var target = CreateEvent(quantityDetails:
            [CreateDetail(quantity: -3, state: InventoryEventStockState.AvailableToSell, status: InventoryEventStockStatus.Pickable)]);
        var sut = CreateHandler(out var itemStockInventoryService, out _, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await itemStockInventoryService.Received(1).ApplyStockSyncAsync(
            FulfilmentLocationIds.BrzDc3PlFulfilmentId, "SKU-1", "TH", "NON", 0, 0, 0, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync Business Rule 4/Calculation 2 non-sellable path creates a new extended record when none exists")]
    public async Task HandleAsync_NonSellableStateNoExistingRecord_CreatesExtendedRecord()
    {
        var target = CreateEvent(quantityDetails:
            [CreateDetail(quantity: 5, state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Held)]);
        var sut = CreateHandler(out _, out var extendedRepository, out _, out _, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await extendedRepository.Received(1).CreateAsync(
            Arg.Is<ItemStockInventoryExtended>(e =>
                e.Qty == 5 &&
                e.FulfilmentId == FulfilmentLocationIds.BrzDc3PlFulfilmentId &&
                e.ItemCode == "SKU-1" &&
                e.COO == "TH" &&
                e.Hallmark == "NON" &&
                e.State == DomainEnums.State.AVAILABLE &&
                e.Status == DomainEnums.Status.HELD),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync Calculation 2 patches Increment(\"/Qty\", delta) when an existing extended record's quantity differs")]
    public async Task HandleAsync_NonSellableStateExistingRecordWithDelta_PatchesIncrementByDelta()
    {
        var existing = new ItemStockInventoryExtended
        {
            FulfilmentId = FulfilmentLocationIds.BrzDc3PlFulfilmentId,
            ItemCode = "SKU-1",
            COO = "TH",
            Hallmark = "NON",
            State = DomainEnums.State.AVAILABLE,
            Status = DomainEnums.Status.HELD,
            Qty = 2,
            ETag = "etag-1",
        };
        var target = CreateEvent(quantityDetails:
            [CreateDetail(quantity: 5, state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Held)]);
        var sut = CreateHandler(out _, out var extendedRepository, out _, out _, out _);
        extendedRepository.GetAsync(
            FulfilmentLocationIds.BrzDc3PlFulfilmentId, "SKU-1", "NON", "TH",
            DomainEnums.State.AVAILABLE, DomainEnums.Status.HELD, Arg.Any<CancellationToken>())
            .Returns(existing);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await extendedRepository.Received(1).PatchAsync(
            existing.Id, existing.Id, "etag-1",
            Arg.Is<IReadOnlyList<PatchOperation>>(ops =>
                ops.Count == 2 && ops.Any(op => op.OperationType == PatchOperationType.Increment)),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync Calculation 2 skips the Patch entirely when the existing extended record's quantity has no discrepancy")]
    public async Task HandleAsync_NonSellableStateExistingRecordNoDelta_SkipsPatch()
    {
        var existing = new ItemStockInventoryExtended
        {
            FulfilmentId = FulfilmentLocationIds.BrzDc3PlFulfilmentId,
            ItemCode = "SKU-1",
            COO = "TH",
            Hallmark = "NON",
            State = DomainEnums.State.AVAILABLE,
            Status = DomainEnums.Status.HELD,
            Qty = 5,
            ETag = "etag-1",
        };
        var target = CreateEvent(quantityDetails:
            [CreateDetail(quantity: 5, state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Held)]);
        var sut = CreateHandler(out _, out var extendedRepository, out _, out _, out _);
        extendedRepository.GetAsync(
            FulfilmentLocationIds.BrzDc3PlFulfilmentId, "SKU-1", "NON", "TH",
            DomainEnums.State.AVAILABLE, DomainEnums.Status.HELD, Arg.Any<CancellationToken>())
            .Returns(existing);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await extendedRepository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
        await extendedRepository.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
    }

    [Fact(DisplayName = "HandleAsync retries the extended-record patch against fresh state after a concurrency conflict and succeeds")]
    public async Task HandleAsync_ConcurrencyConflictOnFirstPatchAttempt_RetriesAndSucceeds()
    {
        var stale = new ItemStockInventoryExtended
        {
            FulfilmentId = FulfilmentLocationIds.BrzDc3PlFulfilmentId, ItemCode = "SKU-1", COO = "TH", Hallmark = "NON",
            State = DomainEnums.State.AVAILABLE, Status = DomainEnums.Status.HELD, Qty = 2, ETag = "etag-1",
        };
        var fresh = new ItemStockInventoryExtended
        {
            FulfilmentId = FulfilmentLocationIds.BrzDc3PlFulfilmentId, ItemCode = "SKU-1", COO = "TH", Hallmark = "NON",
            State = DomainEnums.State.AVAILABLE, Status = DomainEnums.Status.HELD, Qty = 2, ETag = "etag-2",
        };
        var target = CreateEvent(quantityDetails:
            [CreateDetail(quantity: 5, state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Held)]);
        var sut = CreateHandler(out _, out var extendedRepository, out _, out _, out _);
        extendedRepository.GetAsync(
            FulfilmentLocationIds.BrzDc3PlFulfilmentId, "SKU-1", "NON", "TH",
            DomainEnums.State.AVAILABLE, DomainEnums.Status.HELD, Arg.Any<CancellationToken>())
            .Returns(stale, fresh);
        extendedRepository.PatchAsync(
            stale.Id, stale.Id, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(stale.Id, "etag-1"));
        extendedRepository.PatchAsync(
            fresh.Id, fresh.Id, "etag-2", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(fresh);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await extendedRepository.Received(2).GetAsync(
            FulfilmentLocationIds.BrzDc3PlFulfilmentId, "SKU-1", "NON", "TH",
            DomainEnums.State.AVAILABLE, DomainEnums.Status.HELD, Arg.Any<CancellationToken>());
        await extendedRepository.Received(1).PatchAsync(
            fresh.Id, fresh.Id, "etag-2", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync rethrows ConcurrencyException once the extended-record patch retries are exhausted")]
    public async Task HandleAsync_ConcurrencyConflictOnEveryPatchAttempt_ThrowsAfterExhaustingRetries()
    {
        var existing = new ItemStockInventoryExtended
        {
            FulfilmentId = FulfilmentLocationIds.BrzDc3PlFulfilmentId, ItemCode = "SKU-1", COO = "TH", Hallmark = "NON",
            State = DomainEnums.State.AVAILABLE, Status = DomainEnums.Status.HELD, Qty = 2, ETag = "etag-x",
        };
        var target = CreateEvent(quantityDetails:
            [CreateDetail(quantity: 5, state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Held)]);
        var sut = CreateHandler(out _, out var extendedRepository, out _, out _, out _);
        extendedRepository.GetAsync(
            FulfilmentLocationIds.BrzDc3PlFulfilmentId, "SKU-1", "NON", "TH",
            DomainEnums.State.AVAILABLE, DomainEnums.Status.HELD, Arg.Any<CancellationToken>())
            .Returns(existing);
        extendedRepository.PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(existing.Id, "etag-x"));

        await Assert.ThrowsAsync<ConcurrencyException>(
            () => sut.HandleAsync(target, "corr-1", CancellationToken.None));

        await extendedRepository.Received(3).GetAsync(
            FulfilmentLocationIds.BrzDc3PlFulfilmentId, "SKU-1", "NON", "TH",
            DomainEnums.State.AVAILABLE, DomainEnums.Status.HELD, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §7.3 publishes the B2C notification unconditionally per group after the sellable/non-sellable writes")]
    public async Task HandleAsync_AnyGroup_PublishesB2CNotificationUnconditionally()
    {
        var target = CreateEvent(referenceId: "REF-1");
        var sut = CreateHandler(out _, out _, out _, out var omsPublisher, out _);

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await omsPublisher.Received(1).PublishAsync(
            FulfilmentLocationIds.BrzDc3PlFulfilmentId,
            InventoryEventLocationType.ThirdPartyLogistics.ToString(),
            "SKU-1",
            "EA",
            "ORG-1",
            "1234567890",
            Arg.Is<IReadOnlyList<StockOnHandUpdatedOmsQuantityDetail>>(d => d.Count == 1 && d[0].Quantity == 5),
            nameof(StockOnHandUpdatedReason.Receipt),
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            "REF-1:TH:NON",
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §9 publishes the ICR snapshot when EnableSnapshotForIcr is set")]
    public async Task HandleAsync_EnableSnapshotForIcr_PublishesIcrSnapshot()
    {
        var target = CreateEvent();
        var sut = CreateHandler(
            out _, out _, out _, out _, out var icrPublisher,
            featureFlags: new FeatureFlagsOptions { EnableSnapshotForIcr = true });

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await icrPublisher.Received(1).PublishAsync(
            FulfilmentLocationIds.BrzDc3PlFulfilmentId, "SKU-1", "NON", "TH", false, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §9 skips the ICR snapshot publish when EnableSnapshotForIcr is disabled")]
    public async Task HandleAsync_SnapshotForIcrDisabled_SkipsIcrSnapshotPublish()
    {
        var target = CreateEvent();
        var sut = CreateHandler(
            out _, out _, out _, out _, out var icrPublisher,
            featureFlags: new FeatureFlagsOptions());

        await sut.HandleAsync(target, "corr-1", CancellationToken.None);

        await icrPublisher.DidNotReceiveWithAnyArgs().PublishAsync(default!, default!, default!, default!, default, default);
    }

    [Fact(DisplayName = "HandleAsync §8 runs the non-sellable path and rethrows even when the sellable path fails")]
    public async Task HandleAsync_SellablePathThrows_NonSellablePathStillRunsAndExceptionRethrown()
    {
        var target = CreateEvent(quantityDetails:
        [
            CreateDetail(quantity: 3, state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Prepared),
            CreateDetail(quantity: 5, state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Held),
        ]);
        var sut = CreateHandler(out var itemStockInventoryService, out var extendedRepository, out _, out _, out _);
        itemStockInventoryService.ApplyStockSyncAsync(
            default!, default!, default!, default!, default, default, default, default)
            .ThrowsForAnyArgs(new InvalidOperationException("sellable failure"));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => sut.HandleAsync(target, "corr-1", CancellationToken.None));

        await extendedRepository.Received(1).CreateAsync(
            Arg.Is<ItemStockInventoryExtended>(e => e.Qty == 5), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "HandleAsync §8 prioritizes ConcurrencyException over an unrelated fault when both paths fail")]
    public async Task HandleAsync_BothPathsFailWithConcurrencyAndOtherException_RethrowsConcurrencyExceptionFirst()
    {
        var target = CreateEvent(quantityDetails:
        [
            CreateDetail(quantity: 3, state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Prepared),
            CreateDetail(quantity: 5, state: InventoryEventStockState.Available, status: InventoryEventStockStatus.Held),
        ]);
        var sut = CreateHandler(out var itemStockInventoryService, out var extendedRepository, out _, out _, out _);
        itemStockInventoryService.ApplyStockSyncAsync(
            default!, default!, default!, default!, default, default, default, default)
            .ThrowsForAnyArgs(new InvalidOperationException("sellable failure"));
        extendedRepository.GetAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<DomainEnums.State>(), Arg.Any<DomainEnums.Status>(), Arg.Any<CancellationToken>())
            .Returns(new ItemStockInventoryExtended { Qty = 1, ETag = "etag-x" });
        extendedRepository.PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .ThrowsForAnyArgs(new ConcurrencyException("some-id", "etag-x"));

        await Assert.ThrowsAsync<ConcurrencyException>(
            () => sut.HandleAsync(target, "corr-1", CancellationToken.None));
    }
}
