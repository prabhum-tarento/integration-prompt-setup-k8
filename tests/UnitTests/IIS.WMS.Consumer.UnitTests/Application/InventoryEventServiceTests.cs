using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.Exceptions;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Use-case orchestration tests for <see cref="InventoryEventService"/>, with the repository and domain-event dispatcher mocked.</summary>
public class InventoryEventServiceTests
{
    private static readonly DateTimeOffset Now = new(2026, 7, 8, 12, 0, 0, TimeSpan.Zero);

    private readonly IInventoryEventRepository repository = Substitute.For<IInventoryEventRepository>();
    private readonly IDomainEventDispatcher domainEventDispatcher = Substitute.For<IDomainEventDispatcher>();
    private readonly TimeProvider timeProvider = Substitute.For<TimeProvider>();
    private readonly InventoryEventService sut;

    public InventoryEventServiceTests()
    {
        timeProvider.GetUtcNow().Returns(Now);
        sut = new InventoryEventService(
            repository, domainEventDispatcher, timeProvider, Substitute.For<ILogger<InventoryEventService>>());
    }

    [Fact(DisplayName = "GetAsync returns null when the aggregate does not exist")]
    public async Task GetAsync_AggregateDoesNotExist_ReturnsNull()
    {
        repository.GetAsync("WH1:SKU1", "WH1:SKU1", Arg.Any<CancellationToken>()).Returns((InventoryEvent?)null);

        var result = await sut.GetAsync("WH1", "SKU1");

        Assert.Null(result);
    }

    [Fact(DisplayName = "GetAsync maps the aggregate to a response when it exists")]
    public async Task GetAsync_AggregateExists_ReturnsMappedResponse()
    {
        var aggregate = InventoryEvent.Create("WH1:SKU1", "WH1", "SKU1", 10, Now.UtcDateTime);
        repository.GetAsync("WH1:SKU1", "WH1:SKU1", Arg.Any<CancellationToken>()).Returns(aggregate);

        var result = await sut.GetAsync("WH1", "SKU1");

        Assert.NotNull(result);
        Assert.Equal(10, result!.OnHandQuantity);
    }

    [Fact(DisplayName = "CreateAsync persists a deterministic id derived from warehouse and SKU")]
    public async Task CreateAsync_ValidRequest_PersistsAggregateWithDeterministicId()
    {
        repository.CreateAsync(Arg.Any<InventoryEvent>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => callInfo.Arg<InventoryEvent>());

        var result = await sut.CreateAsync(new CreateInventoryEventRequest("WH1", "SKU1", 25));

        Assert.Equal("WH1:SKU1", result.Id);
        Assert.Equal(25, result.OnHandQuantity);
    }

    [Fact(DisplayName = "ReserveStockAsync throws NotFoundException when the aggregate does not exist")]
    public async Task ReserveStockAsync_AggregateDoesNotExist_ThrowsNotFoundException()
    {
        repository.GetAsync("WH1:SKU1", "WH1:SKU1", Arg.Any<CancellationToken>()).Returns((InventoryEvent?)null);

        await Assert.ThrowsAsync<NotFoundException>(
            () => sut.ReserveStockAsync("WH1", "SKU1", new ReserveStockRequest("reservation-1", 5)));
    }

    [Fact(DisplayName = "ReserveStockAsync replaces the aggregate and dispatches its domain events on success")]
    public async Task ReserveStockAsync_SufficientStock_ReplacesAggregateAndDispatchesDomainEvents()
    {
        var aggregate = InventoryEvent.Rehydrate("WH1:SKU1", "WH1", "SKU1", 10, Now.UtcDateTime, Now.UtcDateTime);
        aggregate.ETag = "etag-1";
        repository.GetAsync("WH1:SKU1", "WH1:SKU1", Arg.Any<CancellationToken>()).Returns(aggregate);
        repository.ReplaceAsync(Arg.Any<InventoryEvent>(), "etag-1", Arg.Any<CancellationToken>())
            .Returns(callInfo => callInfo.Arg<InventoryEvent>());

        var result = await sut.ReserveStockAsync("WH1", "SKU1", new ReserveStockRequest("reservation-1", 4));

        Assert.Equal(6, result.OnHandQuantity);
        await domainEventDispatcher.Received(1).DispatchAsync(Arg.Any<IReadOnlyCollection<IDomainEvent>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ReserveStockAsync retries against fresh state after a concurrency conflict")]
    public async Task ReserveStockAsync_ConcurrencyConflictOnFirstAttempt_RetriesAndSucceeds()
    {
        var staleAggregate = InventoryEvent.Rehydrate("WH1:SKU1", "WH1", "SKU1", 10, Now.UtcDateTime, Now.UtcDateTime);
        staleAggregate.ETag = "stale-etag";
        var freshAggregate = InventoryEvent.Rehydrate("WH1:SKU1", "WH1", "SKU1", 10, Now.UtcDateTime, Now.UtcDateTime);
        freshAggregate.ETag = "fresh-etag";

        repository.GetAsync("WH1:SKU1", "WH1:SKU1", Arg.Any<CancellationToken>())
            .Returns(staleAggregate, freshAggregate);
        repository.ReplaceAsync(Arg.Any<InventoryEvent>(), "stale-etag", Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException("WH1:SKU1", "stale-etag"));
        repository.ReplaceAsync(Arg.Any<InventoryEvent>(), "fresh-etag", Arg.Any<CancellationToken>())
            .Returns(callInfo => callInfo.Arg<InventoryEvent>());

        var result = await sut.ReserveStockAsync("WH1", "SKU1", new ReserveStockRequest("reservation-1", 4));

        Assert.Equal(6, result.OnHandQuantity);
        await repository.Received(2).GetAsync("WH1:SKU1", "WH1:SKU1", Arg.Any<CancellationToken>());
    }
}
