using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>
/// §3.4 B2C extension calculation tests for <see cref="ItemStockInventoryExtensionCalculationService"/>
/// (docs/InventoryStateChangedFullQueueTrigger.md), with the segmentation repositories mocked.
/// </summary>
public class ItemStockInventoryExtensionCalculationServiceTests
{
    private static readonly DateTime Now = new(2026, 7, 8, 12, 0, 0, DateTimeKind.Utc);

    private readonly IItemLevelSegmentationRepository itemLevelSegmentationRepository = Substitute.For<IItemLevelSegmentationRepository>();
    private readonly IFulfilmentLevelSegmentationRepository fulfilmentLevelSegmentationRepository = Substitute.For<IFulfilmentLevelSegmentationRepository>();
    private readonly ItemStockInventoryExtensionCalculationService sut;

    public ItemStockInventoryExtensionCalculationServiceTests()
    {
        sut = new ItemStockInventoryExtensionCalculationService(
            itemLevelSegmentationRepository, fulfilmentLevelSegmentationRepository,
            NullLogger<ItemStockInventoryExtensionCalculationService>.Instance);

        itemLevelSegmentationRepository.GetItemLevelFulfilmentyByCategory(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>())
            .Returns((ItemLevelSegmentation?)null);
        fulfilmentLevelSegmentationRepository.GetFulfilmentLevelFulfilmentyByCategory(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns((FulfilmentLevelSegmentationStoreLeveragePercentage?)null);
    }

    private static ItemStockInventory CreateAggregate(
        int b2bAvailable, int b2bAllocated, int b2bUsedShare, int b2bPrepared, int b2cOriginal) =>
        ItemStockInventory.Rehydrate(
            "WH1:SKU1:925:TH", "WH1", "SKU1", "TH", "925",
            b2bAvailable: b2bAvailable, b2cAvailable: b2cOriginal, b2cOriginal: b2cOriginal, b2cExtended: 0,
            b2cAllocated: 0, b2bAllocated: b2bAllocated, b2cPrepared: 0, b2bPrepared: b2bPrepared,
            internalHallmarkAllocated: 0, inTransit: 0, b2cThreshold: 0, isExtended: true, b2bUsedShare: b2bUsedShare,
            inspection: 0, psc: 0, isPosm: false, modifiedUtc: Now);

    [Fact(DisplayName = "CalculateB2CExtensionAsync matches the doc's §3.4 worked example: B2CExtended=260, new B2CAvailable=320, delta=+260")]
    public async Task CalculateB2CExtensionAsync_DocWorkedExample_ProducesExpectedDeltaAndB2CAvailable()
    {
        var aggregate = CreateAggregate(b2bAvailable: 500, b2bAllocated: 200, b2bUsedShare: 40, b2bPrepared: 0, b2cOriginal: 60);
        var deltaResult = new ItemStockInventoryDeltaResult();

        await sut.CalculateB2CExtensionAsync(prevB2CAvailable: 60, aggregate, deltaResult, CancellationToken.None);

        Assert.Equal(260, aggregate.B2CExtended);
        Assert.Equal(320, aggregate.B2CAvailable);
        Assert.True(deltaResult.IsB2CChanged);
        Assert.Equal(260, deltaResult.DeltaTowardsOms);
    }

    [Fact(DisplayName = "CalculateB2CExtensionAsync leaves IsB2CChanged false and DeltaTowardsOms zero when the recalculated B2CAvailable is unchanged")]
    public async Task CalculateB2CExtensionAsync_RecalculatedValueUnchanged_LeavesDeltaResultUntouched()
    {
        var aggregate = CreateAggregate(b2bAvailable: 200, b2bAllocated: 200, b2bUsedShare: 0, b2bPrepared: 0, b2cOriginal: 60);
        var deltaResult = new ItemStockInventoryDeltaResult();

        await sut.CalculateB2CExtensionAsync(prevB2CAvailable: 60, aggregate, deltaResult, CancellationToken.None);

        Assert.Equal(0, aggregate.B2CExtended);
        Assert.Equal(60, aggregate.B2CAvailable);
        Assert.False(deltaResult.IsB2CChanged);
        Assert.Equal(0, deltaResult.DeltaTowardsOms);
    }

    [Fact(DisplayName = "CalculateB2CExtensionAsync checks the item-level rule before falling back to the fulfilment-level rule")]
    public async Task CalculateB2CExtensionAsync_Always_ChecksItemLevelRuleFirst()
    {
        var aggregate = CreateAggregate(b2bAvailable: 500, b2bAllocated: 200, b2bUsedShare: 40, b2bPrepared: 0, b2cOriginal: 60);
        var deltaResult = new ItemStockInventoryDeltaResult();

        await sut.CalculateB2CExtensionAsync(prevB2CAvailable: 60, aggregate, deltaResult, CancellationToken.None);

        await itemLevelSegmentationRepository.Received(1).GetItemLevelFulfilmentyByCategory(
            "WH1", "925", "SKU1", "TH");
    }

    [Fact(DisplayName = "CalculateB2CExtensionAsync skips the fulfilment-level fallback when an active item-level rule exists")]
    public async Task CalculateB2CExtensionAsync_ActiveItemLevelRuleExists_SkipsFulfilmentLevelFallback()
    {
        itemLevelSegmentationRepository.GetItemLevelFulfilmentyByCategory("WH1", "925", "SKU1", "TH")
            .Returns(new ItemLevelSegmentation { IsActive = true, StoreLeveragePercentage = 90 });
        var aggregate = CreateAggregate(b2bAvailable: 500, b2bAllocated: 200, b2bUsedShare: 40, b2bPrepared: 0, b2cOriginal: 60);
        var deltaResult = new ItemStockInventoryDeltaResult();

        await sut.CalculateB2CExtensionAsync(prevB2CAvailable: 60, aggregate, deltaResult, CancellationToken.None);

        await fulfilmentLevelSegmentationRepository.DidNotReceiveWithAnyArgs().GetFulfilmentLevelFulfilmentyByCategory(
            default!, default!, default);
    }

    [Fact(DisplayName = "CalculateB2CExtensionAsync falls back to the fulfilment-level rule when no active item-level rule exists")]
    public async Task CalculateB2CExtensionAsync_NoActiveItemLevelRule_FallsBackToFulfilmentLevelRule()
    {
        var aggregate = CreateAggregate(b2bAvailable: 500, b2bAllocated: 200, b2bUsedShare: 40, b2bPrepared: 0, b2cOriginal: 60);
        var deltaResult = new ItemStockInventoryDeltaResult();

        await sut.CalculateB2CExtensionAsync(prevB2CAvailable: 60, aggregate, deltaResult, CancellationToken.None);

        await fulfilmentLevelSegmentationRepository.Received(1).GetFulfilmentLevelFulfilmentyByCategory(
            "WH1", "925", Arg.Any<CancellationToken>());
    }
}
