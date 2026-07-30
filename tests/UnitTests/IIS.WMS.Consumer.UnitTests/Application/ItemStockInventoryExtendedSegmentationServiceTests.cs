using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Enums;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>
/// §3.5 extended-state inventory segmentation tests for
/// <see cref="ItemStockInventoryExtendedSegmentationService"/> (docs/events/inventory.InventoryStateChanged.md),
/// with the repository and correlation context mocked.
/// </summary>
public class ItemStockInventoryExtendedSegmentationServiceTests
{
    private readonly IItemStockInventoryExtendedRepository repository = Substitute.For<IItemStockInventoryExtendedRepository>();
    private readonly ICorrelationContext correlationContext = Substitute.For<ICorrelationContext>();
    private readonly ItemStockInventoryExtendedSegmentationService sut;

    public ItemStockInventoryExtendedSegmentationServiceTests()
    {
        correlationContext.Type.Returns(string.Empty);
        sut = new ItemStockInventoryExtendedSegmentationService(
            repository, correlationContext, Substitute.For<ILogger<ItemStockInventoryExtendedSegmentationService>>());
    }

    [Fact(DisplayName = "ApplyAsync creates a new to-state record when none exists and toState/toStatus is not the baseline Available/Pickable pair")]
    public async Task ApplyAsync_ToStateNotAvailablePickableAndNoExistingRecord_CreatesRecord()
    {
        repository.GetAsync("WH1", "SKU1", "925", "TH", State.BLOCKED, Status.HELD, Arg.Any<CancellationToken>())
            .Returns((ItemStockInventoryExtended?)null);

        await sut.ApplyAsync(
            "WH1", "SKU1", "925", "TH",
            fromState: State.AVAILABLE, fromStatus: Status.PICKABLE,
            toState: State.BLOCKED, toStatus: Status.HELD,
            quantity: 5, CancellationToken.None);

        await repository.Received(1).CreateAsync(
            Arg.Is<ItemStockInventoryExtended>(e => e.Qty == 5 && e.State == State.BLOCKED && e.Status == Status.HELD),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplyAsync increments an existing to-state record's Qty by the inbound quantity")]
    public async Task ApplyAsync_ToStateExistingRecord_IncrementsQty()
    {
        var existing = new ItemStockInventoryExtended
        {
            FulfilmentId = "WH1", ItemCode = "SKU1", Hallmark = "925", COO = "TH",
            State = State.BLOCKED, Status = Status.HELD, Qty = 10, ETag = "etag-1",
        };
        repository.GetAsync("WH1", "SKU1", "925", "TH", State.BLOCKED, Status.HELD, Arg.Any<CancellationToken>())
            .Returns(existing);

        await sut.ApplyAsync(
            "WH1", "SKU1", "925", "TH",
            fromState: State.AVAILABLE, fromStatus: Status.PICKABLE,
            toState: State.BLOCKED, toStatus: Status.HELD,
            quantity: 5, CancellationToken.None);

        Assert.Equal(15, existing.Qty);
        await repository.Received(1).ReplaceAsync(existing, "etag-1", Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplyAsync decrements an existing from-state record when it holds enough quantity")]
    public async Task ApplyAsync_FromStateSufficientQty_Decrements()
    {
        var existing = new ItemStockInventoryExtended
        {
            FulfilmentId = "WH1", ItemCode = "SKU1", Hallmark = "925", COO = "TH",
            State = State.BLOCKED, Status = Status.HELD, Qty = 10, ETag = "etag-1",
        };
        repository.GetAsync("WH1", "SKU1", "925", "TH", State.BLOCKED, Status.HELD, Arg.Any<CancellationToken>())
            .Returns(existing);
        repository.GetAsync("WH1", "SKU1", "925", "TH", State.AVAILABLE, Status.PREPARED, Arg.Any<CancellationToken>())
            .Returns((ItemStockInventoryExtended?)null);

        await sut.ApplyAsync(
            "WH1", "SKU1", "925", "TH",
            fromState: State.BLOCKED, fromStatus: Status.HELD,
            toState: State.AVAILABLE, toStatus: Status.PREPARED,
            quantity: 5, CancellationToken.None);

        Assert.Equal(5, existing.Qty);
        await repository.Received(1).ReplaceAsync(existing, "etag-1", Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplyAsync logs and skips the from-state decrement instead of throwing when it would oversell")]
    public async Task ApplyAsync_FromStateInsufficientQty_SkipsWithoutThrowing()
    {
        var existing = new ItemStockInventoryExtended
        {
            FulfilmentId = "WH1", ItemCode = "SKU1", Hallmark = "925", COO = "TH",
            State = State.BLOCKED, Status = Status.HELD, Qty = 2, ETag = "etag-1",
        };
        repository.GetAsync("WH1", "SKU1", "925", "TH", State.BLOCKED, Status.HELD, Arg.Any<CancellationToken>())
            .Returns(existing);
        repository.GetAsync("WH1", "SKU1", "925", "TH", State.AVAILABLE, Status.PREPARED, Arg.Any<CancellationToken>())
            .Returns((ItemStockInventoryExtended?)null);

        await sut.ApplyAsync(
            "WH1", "SKU1", "925", "TH",
            fromState: State.BLOCKED, fromStatus: Status.HELD,
            toState: State.AVAILABLE, toStatus: Status.PREPARED,
            quantity: 5, CancellationToken.None);

        Assert.Equal(2, existing.Qty);
        await repository.DidNotReceiveWithAnyArgs().ReplaceAsync(default!, default!, default);
    }

    [Fact(DisplayName = "ApplyAsync skips both branches when the transition is exactly the baseline Available/Pickable pair on both sides")]
    public async Task ApplyAsync_BothSidesAvailablePickable_SkipsBothBranches()
    {
        await sut.ApplyAsync(
            "WH1", "SKU1", "925", "TH",
            fromState: State.AVAILABLE, fromStatus: Status.PICKABLE,
            toState: State.AVAILABLE, toStatus: Status.PICKABLE,
            quantity: 5, CancellationToken.None);

        await repository.DidNotReceiveWithAnyArgs().GetAsync(
            default!, default!, default!, default!, default, default, default);
        await repository.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
        await repository.DidNotReceiveWithAnyArgs().ReplaceAsync(default!, default!, default);
    }

    [Fact(DisplayName = "ApplyAsync skips the from-state branch when the correlation context type is an InventoryAdjusted redelivery")]
    public async Task ApplyAsync_InventoryAdjustedRedelivery_SkipsFromStateBranch()
    {
        correlationContext.Type.Returns(KafkaEvents.InventoryAdjustedEventType);
        repository.GetAsync("WH1", "SKU1", "925", "TH", State.BLOCKED, Status.HELD, Arg.Any<CancellationToken>())
            .Returns((ItemStockInventoryExtended?)null);

        await sut.ApplyAsync(
            "WH1", "SKU1", "925", "TH",
            fromState: State.BLOCKED, fromStatus: Status.HELD,
            toState: State.AVAILABLE, toStatus: Status.PICKABLE,
            quantity: 5, CancellationToken.None);

        await repository.DidNotReceiveWithAnyArgs().GetAsync(
            "WH1", "SKU1", "925", "TH", State.BLOCKED, Status.HELD, Arg.Any<CancellationToken>());
    }
}
