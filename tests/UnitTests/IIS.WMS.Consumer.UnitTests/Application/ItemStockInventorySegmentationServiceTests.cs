using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>
/// §3.3 segmentation/extension orchestration tests for <see cref="ItemStockInventorySegmentationService"/>
/// (docs/InventoryStateChangedFullQueueTrigger.md), with the repositories mocked.
/// </summary>
public class ItemStockInventorySegmentationServiceTests
{
    private static readonly DateTimeOffset Now = new(2026, 7, 8, 12, 0, 0, TimeSpan.Zero);
    private const string Id = "WH1:SKU1:925:TH";

    private readonly IItemStockInventoryRepository repository = Substitute.For<IItemStockInventoryRepository>();
    private readonly IItemLevelSegmentationRepository itemLevelSegmentationRepository = Substitute.For<IItemLevelSegmentationRepository>();
    private readonly TimeProvider timeProvider = Substitute.For<TimeProvider>();
    private readonly ItemStockInventorySegmentationService sut;

    public ItemStockInventorySegmentationServiceTests()
    {
        timeProvider.GetUtcNow().Returns(Now);
        sut = new ItemStockInventorySegmentationService(
            repository, itemLevelSegmentationRepository, timeProvider,
            Substitute.For<ILogger<ItemStockInventorySegmentationService>>());

        itemLevelSegmentationRepository.GetItemLevelFulfilmentyByCategory(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>())
            .Returns((ItemLevelSegmentation?)null);
    }

    private static ItemStockInventory CreateAggregate(
        string etag, int b2bAvailable = 20, int b2cAvailable = 20, int b2cAllocated = 0, int b2cPrepared = 0)
    {
        var aggregate = ItemStockInventory.Rehydrate(
            Id, "WH1", "SKU1", "TH", "925",
            b2bAvailable: b2bAvailable, b2cAvailable: b2cAvailable, b2cOriginal: b2cAvailable, b2cExtended: 0,
            b2cAllocated: b2cAllocated, b2bAllocated: 0, b2cPrepared: b2cPrepared, b2bPrepared: 0,
            internalHallmarkAllocated: 0, inTransit: 0, b2cThreshold: 0, isExtended: false, b2bUsedShare: 0,
            inspection: 0, psc: 0, isPosm: false, modifiedUtc: Now.UtcDateTime);
        aggregate.ETag = etag;

        return aggregate;
    }

    [Fact(DisplayName = "ApplySegmentationAsync creates a zero-initialized record and skips a negative inbound quantity")]
    public async Task ApplySegmentationAsync_NoExistingRecordAndNegativeInboundQty_SkipsWithoutCreating()
    {
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns((ItemStockInventory?)null);

        var result = await sut.ApplySegmentationAsync("WH1", "SKU1", "TH", "925", -5, isThirdPartyLogistics: false, CancellationToken.None);

        Assert.False(result.IsB2CChanged);
        await repository.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
    }

    [Fact(DisplayName = "ApplySegmentationAsync applies 3PL B2C segmentation and persists via CreateAsync when no record exists")]
    public async Task ApplySegmentationAsync_ThirdPartyLogisticsAndNoExistingRecord_CreatesAndAppliesB2CSegmentation()
    {
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns((ItemStockInventory?)null);
        repository.CreateAsync(Arg.Any<ItemStockInventory>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => callInfo.Arg<ItemStockInventory>());

        var result = await sut.ApplySegmentationAsync("WH1", "SKU1", "TH", "925", 5, isThirdPartyLogistics: true, CancellationToken.None);

        Assert.True(result.IsB2CChanged);
        Assert.Equal(5, result.DeltaTowardsOms);
        await repository.Received(1).CreateAsync(
            Arg.Is<ItemStockInventory>(a => a.B2CAvailable == 5), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplySegmentationAsync applies fulfilment-level segmentation and patches an existing record when no item-level rule is active")]
    public async Task ApplySegmentationAsync_ExistingRecordNoActiveItemLevelRule_AppliesFulfilmentLevelSegmentationAndPatches()
    {
        var aggregate = CreateAggregate("etag-1", b2bAvailable: 20);
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);
        repository.PatchAsync(Id, Id, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);

        var result = await sut.ApplySegmentationAsync("WH1", "SKU1", "TH", "925", 5, isThirdPartyLogistics: false, CancellationToken.None);

        Assert.Equal(25, aggregate.B2BAvailable);
        Assert.False(result.IsB2CChanged);
        await repository.Received(1).PatchAsync(
            Id, Id, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplySegmentationAsync activates extension and applies item-level extension when an active item-level rule exists")]
    public async Task ApplySegmentationAsync_ActiveItemLevelRuleExists_ActivatesExtensionAndAppliesItemLevelExtension()
    {
        var aggregate = CreateAggregate("etag-1", b2cAvailable: 50, b2cAllocated: 0, b2cPrepared: 0);
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);
        repository.PatchAsync(Id, Id, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);
        itemLevelSegmentationRepository.GetItemLevelFulfilmentyByCategory("WH1", "925", "SKU1", "TH")
            .Returns(new ItemLevelSegmentation { IsActive = true, EcomShare = 90 });

        var result = await sut.ApplySegmentationAsync("WH1", "SKU1", "TH", "925", 5, isThirdPartyLogistics: false, CancellationToken.None);

        Assert.True(aggregate.IsExtended);
        Assert.True(result.IsB2CChanged);
        Assert.Equal(5, result.DeltaTowardsOms);
        Assert.Equal(55, aggregate.B2COriginal);
        Assert.Equal(55, aggregate.B2CAvailable);
    }

    [Fact(DisplayName = "ApplySegmentationAsync writes back the item-level rule when the fulfilment id is not TDC")]
    public async Task ApplySegmentationAsync_FulfilmentIdNotTdc_WritesBackItemLevelRule()
    {
        var aggregate = CreateAggregate("etag-1");
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);
        repository.PatchAsync(Id, Id, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);
        var rule = new ItemLevelSegmentation { IsActive = false };
        itemLevelSegmentationRepository.GetItemLevelFulfilmentyByCategory("WH1", "925", "SKU1", "TH").Returns(rule);

        await sut.ApplySegmentationAsync("WH1", "SKU1", "TH", "925", 5, isThirdPartyLogistics: false, CancellationToken.None);

        await itemLevelSegmentationRepository.Received(1).UpdateItemLevelFulfilmentAsync(rule, Arg.Any<CancellationToken>());
        Assert.True(rule.IsExtended);
    }

    [Fact(DisplayName = "ApplySegmentationAsync skips the item-level rule write-back when the fulfilment id is TDC")]
    public async Task ApplySegmentationAsync_FulfilmentIdIsTdc_SkipsItemLevelRuleWriteBack()
    {
        var aggregate = ItemStockInventory.Rehydrate(
            "TDC:SKU1:925:TH", "TDC", "SKU1", "TH", "925",
            b2bAvailable: 20, b2cAvailable: 20, b2cOriginal: 20, b2cExtended: 0,
            b2cAllocated: 0, b2bAllocated: 0, b2cPrepared: 0, b2bPrepared: 0,
            internalHallmarkAllocated: 0, inTransit: 0, b2cThreshold: 0, isExtended: false, b2bUsedShare: 0,
            inspection: 0, psc: 0, isPosm: false, modifiedUtc: Now.UtcDateTime);
        aggregate.ETag = "etag-1";
        repository.GetAsync("TDC:SKU1:925:TH", "TDC:SKU1:925:TH", Arg.Any<CancellationToken>()).Returns(aggregate);
        repository.PatchAsync(
            "TDC:SKU1:925:TH", "TDC:SKU1:925:TH", "etag-1",
            Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);

        await sut.ApplySegmentationAsync("TDC", "SKU1", "TH", "925", 5, isThirdPartyLogistics: false, CancellationToken.None);

        await itemLevelSegmentationRepository.DidNotReceiveWithAnyArgs().UpdateItemLevelFulfilmentAsync(default!, default);
    }

    [Fact(DisplayName = "ApplySegmentationAsync retries against fresh state after a concurrency conflict")]
    public async Task ApplySegmentationAsync_ConcurrencyConflictOnFirstAttempt_RetriesAndSucceeds()
    {
        var staleAggregate = CreateAggregate("stale-etag");
        var freshAggregate = CreateAggregate("fresh-etag");
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(staleAggregate, freshAggregate);
        repository.PatchAsync(Id, Id, "stale-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(Id, "stale-etag"));
        repository.PatchAsync(Id, Id, "fresh-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(freshAggregate);

        await sut.ApplySegmentationAsync("WH1", "SKU1", "TH", "925", 5, isThirdPartyLogistics: false, CancellationToken.None);

        await repository.Received(2).GetAsync(Id, Id, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplySegmentationAsync rethrows ConcurrencyException once retries are exhausted")]
    public async Task ApplySegmentationAsync_ConcurrencyConflictOnEveryAttempt_ThrowsAfterExhaustingRetries()
    {
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(_ => CreateAggregate("etag-x"));
        repository.PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(Id, "etag-x"));

        await Assert.ThrowsAsync<ConcurrencyException>(
            () => sut.ApplySegmentationAsync("WH1", "SKU1", "TH", "925", 5, isThirdPartyLogistics: false, CancellationToken.None));

        await repository.Received(3).GetAsync(Id, Id, Arg.Any<CancellationToken>());
    }
}
