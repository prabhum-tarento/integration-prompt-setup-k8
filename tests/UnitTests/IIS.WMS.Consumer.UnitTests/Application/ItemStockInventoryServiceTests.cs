using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>
/// Use-case orchestration tests for <see cref="ItemStockInventoryService"/>'s re-read-and-reapply
/// retry loop, with the repository and domain-event dispatcher mocked - the fix for the "PreCondition
/// failed" issue this port addresses (integration-resiliency.instructions.md §2).
/// </summary>
public class ItemStockInventoryServiceTests
{
    private static readonly DateTimeOffset Now = new(2026, 7, 8, 12, 0, 0, TimeSpan.Zero);
    private const string Id = "WH1:SKU1:925:TH";

    private readonly IItemStockInventoryRepository repository = Substitute.For<IItemStockInventoryRepository>();
    private readonly IDomainEventDispatcher domainEventDispatcher = Substitute.For<IDomainEventDispatcher>();
    private readonly TimeProvider timeProvider = Substitute.For<TimeProvider>();
    private readonly ItemStockInventoryService sut;

    public ItemStockInventoryServiceTests()
    {
        timeProvider.GetUtcNow().Returns(Now);
        sut = new ItemStockInventoryService(
            repository, domainEventDispatcher, timeProvider, Substitute.For<ILogger<ItemStockInventoryService>>());
    }

    private static ItemStockInventory CreateAggregate(string etag, int b2bAllocated = 10, int b2cAllocated = 10)
    {
        var aggregate = ItemStockInventory.Rehydrate(
            Id, "WH1", "SKU1", "TH", "925",
            b2bAvailable: 20, b2cAvailable: 20, b2cOriginal: 20, b2cExtended: 0,
            b2cAllocated: b2cAllocated, b2bAllocated: b2bAllocated, b2cPrepared: 0, b2bPrepared: 0,
            internalHallmarkAllocated: 0, inTransit: 0, b2cThreshold: 0, isExtended: false, b2bUsedShare: 0,
            inspection: 0, psc: 0, isPosm: false, modifiedUtc: Now.UtcDateTime);
        aggregate.ETag = etag;

        return aggregate;
    }

    [Fact(DisplayName = "ApplyPickAsync replaces the aggregate and dispatches its domain events on success")]
    public async Task ApplyPickAsync_SufficientAllocated_ReplacesAggregateAndDispatchesDomainEvents()
    {
        var aggregate = CreateAggregate("etag-1", b2bAllocated: 10);
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);
        repository.ReplaceAsync(aggregate, "etag-1", Arg.Any<CancellationToken>()).Returns(aggregate);

        await sut.ApplyPickAsync("WH1", "SKU1", "TH", "925", ItemStockPickChannel.B2B, 4, CancellationToken.None);

        Assert.Equal(6, aggregate.B2BAllocated);
        await repository.Received(1).ReplaceAsync(aggregate, "etag-1", Arg.Any<CancellationToken>());
        await domainEventDispatcher.Received(1).DispatchAsync(
            Arg.Is<IReadOnlyCollection<IDomainEvent>>(events => events.Count == 1), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplyPickAsync retries against fresh state after a concurrency conflict")]
    public async Task ApplyPickAsync_ConcurrencyConflictOnFirstAttempt_RetriesAndSucceeds()
    {
        var staleAggregate = CreateAggregate("stale-etag", b2bAllocated: 10);
        var freshAggregate = CreateAggregate("fresh-etag", b2bAllocated: 10);

        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(staleAggregate, freshAggregate);
        repository.ReplaceAsync(staleAggregate, "stale-etag", Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(Id, "stale-etag"));
        repository.ReplaceAsync(freshAggregate, "fresh-etag", Arg.Any<CancellationToken>()).Returns(freshAggregate);

        await sut.ApplyPickAsync("WH1", "SKU1", "TH", "925", ItemStockPickChannel.B2B, 4, CancellationToken.None);

        await repository.Received(2).GetAsync(Id, Id, Arg.Any<CancellationToken>());
        Assert.Equal(6, freshAggregate.B2BAllocated);
    }

    [Fact(DisplayName = "ApplyPickAsync rethrows ConcurrencyException once retries are exhausted")]
    public async Task ApplyPickAsync_ConcurrencyConflictOnEveryAttempt_ThrowsAfterExhaustingRetries()
    {
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>())
            .Returns(_ => CreateAggregate("etag-x", b2bAllocated: 10));
        repository.ReplaceAsync(Arg.Any<ItemStockInventory>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(Id, "etag-x"));

        await Assert.ThrowsAsync<ConcurrencyException>(
            () => sut.ApplyPickAsync("WH1", "SKU1", "TH", "925", ItemStockPickChannel.B2B, 4, CancellationToken.None));

        await repository.Received(3).GetAsync(Id, Id, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplyPickAsync logs and returns without mutating when no record exists")]
    public async Task ApplyPickAsync_NoMatchingRecord_DoesNotReplaceOrDispatch()
    {
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns((ItemStockInventory?)null);

        await sut.ApplyPickAsync("WH1", "SKU1", "TH", "925", ItemStockPickChannel.B2B, 4, CancellationToken.None);

        await repository.DidNotReceiveWithAnyArgs().ReplaceAsync(default!, default!, default);
        await domainEventDispatcher.DidNotReceiveWithAnyArgs().DispatchAsync(default!, default);
    }

    [Fact(DisplayName = "ApplyPickAsync swallows a non-extended oversell instead of rethrowing")]
    public async Task ApplyPickAsync_NonExtendedOversell_LogsAndReturnsWithoutReplacing()
    {
        var aggregate = CreateAggregate("etag-1", b2cAllocated: 2);
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);

        await sut.ApplyPickAsync("WH1", "SKU1", "TH", "925", ItemStockPickChannel.B2C, 5, CancellationToken.None);

        await repository.DidNotReceiveWithAnyArgs().ReplaceAsync(default!, default!, default);
    }

    [Fact(DisplayName = "ApplyUnpickAsync replaces the aggregate and dispatches its domain events on success")]
    public async Task ApplyUnpickAsync_PreparedQuantityAvailable_ReplacesAggregateAndDispatchesDomainEvents()
    {
        var aggregate = CreateAggregate("etag-1", b2bAllocated: 10);
        aggregate.PickB2B(6, Now.UtcDateTime);
        aggregate.ClearDomainEvents();
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);
        repository.ReplaceAsync(aggregate, "etag-1", Arg.Any<CancellationToken>()).Returns(aggregate);

        await sut.ApplyUnpickAsync("WH1", "SKU1", "TH", "925", 4, CancellationToken.None);

        Assert.Equal(2, aggregate.B2BPrepared);
        await repository.Received(1).ReplaceAsync(aggregate, "etag-1", Arg.Any<CancellationToken>());
        await domainEventDispatcher.Received(1).DispatchAsync(
            Arg.Is<IReadOnlyCollection<IDomainEvent>>(events => events.Count == 1), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ApplyUnpickAsync swallows the reject when nothing is prepared instead of rethrowing")]
    public async Task ApplyUnpickAsync_NothingPrepared_LogsAndReturnsWithoutReplacing()
    {
        var aggregate = CreateAggregate("etag-1");
        repository.GetAsync(Id, Id, Arg.Any<CancellationToken>()).Returns(aggregate);

        await sut.ApplyUnpickAsync("WH1", "SKU1", "TH", "925", 4, CancellationToken.None);

        await repository.DidNotReceiveWithAnyArgs().ReplaceAsync(default!, default!, default);
    }
}
