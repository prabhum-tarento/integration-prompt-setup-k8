using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>
/// Extension-recalculation gating tests for <see cref="ItemStockInventoryExtensionService"/>, with the
/// repository, inner <see cref="IItemStockInventoryService"/>, and extension calculation service mocked.
/// </summary>
public class ItemStockInventoryExtensionServiceTests
{
    private static readonly DateTimeOffset Now = new(2026, 7, 8, 12, 0, 0, TimeSpan.Zero);
    private const string Id = "WH1:SKU1:925:TH";

    private readonly IItemStockInventoryRepository repository = Substitute.For<IItemStockInventoryRepository>();
    private readonly IItemStockInventoryService itemStockInventoryService = Substitute.For<IItemStockInventoryService>();
    private readonly IItemStockInventoryExtensionCalculationService extensionCalculationService =
        Substitute.For<IItemStockInventoryExtensionCalculationService>();
    private readonly ItemStockInventoryExtensionService sut;

    public ItemStockInventoryExtensionServiceTests()
    {
        sut = new ItemStockInventoryExtensionService(
            repository, itemStockInventoryService, extensionCalculationService,
            Substitute.For<ILogger<ItemStockInventoryExtensionService>>());
    }

    private static ItemStockInventory CreateAggregate(string etag, bool isExtended)
    {
        var aggregate = ItemStockInventory.Rehydrate(
            Id, "WH1", "SKU1", "TH", "925",
            b2bAvailable: 20, b2cAvailable: 20, b2cOriginal: 20, b2cExtended: 0,
            b2cAllocated: 0, b2bAllocated: 0, b2cPrepared: 0, b2bPrepared: 0,
            internalHallmarkAllocated: 0, inTransit: 0, b2cThreshold: 0, isExtended: isExtended, b2bUsedShare: 0,
            inspection: 0, psc: 0, isPosm: false, modifiedUtc: Now.UtcDateTime);
        aggregate.ETag = etag;

        return aggregate;
    }

    [Fact(DisplayName = "ApplyPickB2BWithExtensionAsync patches b2CExtended and b2CAVL when the record is extended and the delta changed")]
    public async Task ApplyPickB2BWithExtensionAsync_ExtendedAndB2CChanged_PatchesExtensionFields()
    {
        var aggregate = CreateAggregate("etag-1", isExtended: true);
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);
        extensionCalculationService
            .CalculateB2CExtensionAsync(20, aggregate, Arg.Any<ItemStockInventoryDeltaResult>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                callInfo.Arg<ItemStockInventoryDeltaResult>().IsB2CChanged = true;
                callInfo.Arg<ItemStockInventoryDeltaResult>().DeltaTowardsOms = 3;
                return Task.CompletedTask;
            });

        var result = await sut.ApplyPickB2BWithExtensionAsync("WH1", "SKU1", "TH", "925", 4, CancellationToken.None);

        await itemStockInventoryService.Received(1).ApplyPickAsync(
            "WH1", "SKU1", "TH", "925", ItemStockPickChannel.B2B, 4, Arg.Any<CancellationToken>());
        Assert.True(result.IsB2CChanged);
        Assert.Equal(3, result.DeltaTowardsOms);
        await repository.Received(1).PatchAsync(
            aggregate.Id, aggregate.Category, "etag-1",
            Arg.Is<IReadOnlyList<PatchOperation>>(ops => ops.Count == 2),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplyPickB2BWithExtensionAsync does not patch when the record is not extended")]
    public async Task ApplyPickB2BWithExtensionAsync_NotExtended_DoesNotPatch()
    {
        var aggregate = CreateAggregate("etag-1", isExtended: false);
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);

        await sut.ApplyPickB2BWithExtensionAsync("WH1", "SKU1", "TH", "925", 4, CancellationToken.None);

        await extensionCalculationService.DidNotReceiveWithAnyArgs().CalculateB2CExtensionAsync(
            default, default!, default!, default);
        await repository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }

    [Fact(DisplayName = "ApplyPickB2BWithExtensionAsync does not patch when extended but the delta did not change")]
    public async Task ApplyPickB2BWithExtensionAsync_ExtendedButNoB2CChange_DoesNotPatch()
    {
        var aggregate = CreateAggregate("etag-1", isExtended: true);
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);

        await sut.ApplyPickB2BWithExtensionAsync("WH1", "SKU1", "TH", "925", 4, CancellationToken.None);

        await repository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }

    [Fact(DisplayName = "ApplyPickB2CWithExtensionAsync patches b2CExtended and b2CAVL when the record is extended and the delta changed")]
    public async Task ApplyPickB2CWithExtensionAsync_ExtendedAndB2CChanged_PatchesExtensionFields()
    {
        var aggregate = CreateAggregate("etag-1", isExtended: true);
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);
        extensionCalculationService
            .CalculateB2CExtensionAsync(20, aggregate, Arg.Any<ItemStockInventoryDeltaResult>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                callInfo.Arg<ItemStockInventoryDeltaResult>().IsB2CChanged = true;
                callInfo.Arg<ItemStockInventoryDeltaResult>().DeltaTowardsOms = -2;
                return Task.CompletedTask;
            });

        var result = await sut.ApplyPickB2CWithExtensionAsync("WH1", "SKU1", "TH", "925", 4, CancellationToken.None);

        await itemStockInventoryService.Received(1).ApplyPickAsync(
            "WH1", "SKU1", "TH", "925", ItemStockPickChannel.B2C, 4, Arg.Any<CancellationToken>());
        Assert.True(result.IsB2CChanged);
        Assert.Equal(-2, result.DeltaTowardsOms);
        await repository.Received(1).PatchAsync(
            aggregate.Id, aggregate.Category, "etag-1",
            Arg.Is<IReadOnlyList<PatchOperation>>(ops => ops.Count == 2),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplyPickB2CWithExtensionAsync does not patch when the record is not extended")]
    public async Task ApplyPickB2CWithExtensionAsync_NotExtended_DoesNotPatch()
    {
        var aggregate = CreateAggregate("etag-1", isExtended: false);
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);

        await sut.ApplyPickB2CWithExtensionAsync("WH1", "SKU1", "TH", "925", 4, CancellationToken.None);

        await repository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }

    [Fact(DisplayName = "ApplyUnpickWithExtensionAsync patches b2CExtended and b2CAVL when the record is extended and the delta changed")]
    public async Task ApplyUnpickWithExtensionAsync_ExtendedAndB2CChanged_PatchesExtensionFields()
    {
        var aggregate = CreateAggregate("etag-1", isExtended: true);
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);
        extensionCalculationService
            .CalculateB2CExtensionAsync(20, aggregate, Arg.Any<ItemStockInventoryDeltaResult>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                callInfo.Arg<ItemStockInventoryDeltaResult>().IsB2CChanged = true;
                callInfo.Arg<ItemStockInventoryDeltaResult>().DeltaTowardsOms = 5;
                return Task.CompletedTask;
            });

        var result = await sut.ApplyUnpickWithExtensionAsync("WH1", "SKU1", "TH", "925", 4, CancellationToken.None);

        await itemStockInventoryService.Received(1).ApplyUnpickAsync(
            "WH1", "SKU1", "TH", "925", 4, Arg.Any<CancellationToken>());
        Assert.True(result.IsB2CChanged);
        Assert.Equal(5, result.DeltaTowardsOms);
        await repository.Received(1).PatchAsync(
            aggregate.Id, aggregate.Category, "etag-1",
            Arg.Is<IReadOnlyList<PatchOperation>>(ops => ops.Count == 2),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplyUnpickWithExtensionAsync does not patch when the record is not extended")]
    public async Task ApplyUnpickWithExtensionAsync_NotExtended_DoesNotPatch()
    {
        var aggregate = CreateAggregate("etag-1", isExtended: false);
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);

        await sut.ApplyUnpickWithExtensionAsync("WH1", "SKU1", "TH", "925", 4, CancellationToken.None);

        await repository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }
}
