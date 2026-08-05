using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Domain.Enums;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>
/// Use-case orchestration tests for <see cref="ConsolidatedOrderShippedService.ConfirmAsync"/>
/// (docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.1/§4.1), with the inventory repository,
/// extension-calculation service, and domain-event dispatcher mocked.
/// </summary>
public class ConsolidatedOrderShippedServiceTests
{
    private static readonly DateTimeOffset Now = new(2026, 7, 8, 12, 0, 0, TimeSpan.Zero);
    private const string FulfilmentId = "WH1";
    private const string ItemCode = "SKU1";
    private const string CountryOfOrigin = "TH";
    private const string Hallmark = "925";
    private const string InventoryId = "WH1:SKU1:925:TH";

    private readonly IItemStockInventoryRepository inventoryRepository = Substitute.For<IItemStockInventoryRepository>();
    private readonly IItemStockInventoryExtensionCalculationService extensionCalculationService = Substitute.For<IItemStockInventoryExtensionCalculationService>();
    private readonly IDomainEventDispatcher domainEventDispatcher = Substitute.For<IDomainEventDispatcher>();
    private readonly TimeProvider timeProvider = Substitute.For<TimeProvider>();
    private readonly ConsolidatedOrderShippedService sut;

    public ConsolidatedOrderShippedServiceTests()
    {
        timeProvider.GetUtcNow().Returns(Now);
        sut = new ConsolidatedOrderShippedService(
            inventoryRepository, extensionCalculationService, domainEventDispatcher, timeProvider,
            Substitute.For<ILogger<ConsolidatedOrderShippedService>>());
    }

    private static ItemStockInventory CreateAggregate(
        string etag, int b2bAvailable = 20, int b2bPrepared = 10, int psc = 0, bool isExtended = false) =>
        SetEtag(
            ItemStockInventory.Rehydrate(
                InventoryId, FulfilmentId, ItemCode, CountryOfOrigin, Hallmark,
                b2bAvailable: b2bAvailable, b2cAvailable: 20, b2cOriginal: 20, b2cExtended: 0,
                b2cAllocated: 5, b2bAllocated: 10, b2cPrepared: 0, b2bPrepared: b2bPrepared,
                internalHallmarkAllocated: 0, inTransit: 0, b2cThreshold: 0, isExtended: isExtended, b2bUsedShare: 0,
                inspection: 0, psc: psc, isPosm: false, modifiedUtc: Now.UtcDateTime),
            etag);

    private static ItemStockInventory SetEtag(ItemStockInventory aggregate, string etag)
    {
        aggregate.ETag = etag;
        return aggregate;
    }

    private static B2BOrderConfirmedRequest CreateRequest(
        ConfirmationType confirmationType = ConfirmationType.STANDARD,
        int shippedQuantity = 4,
        int allocatedFromB2BBucketQuantity = 4) => new(
        FulfilmentCode: FulfilmentId,
        ItemCode: ItemCode,
        CountryOfOrigin: CountryOfOrigin,
        Hallmark: Hallmark,
        ShippedQuantity: shippedQuantity,
        ConfirmationType: confirmationType,
        AllocatedFromB2BBucketQuantity: allocatedFromB2BBucketQuantity);

    [Fact(DisplayName = "ConfirmAsync returns a no-change result without patching when ShippedQuantity is not positive")]
    public async Task ConfirmAsync_NonPositiveQuantity_ReturnsNoChangeWithoutPatching()
    {
        var result = await sut.ConfirmAsync(CreateRequest(shippedQuantity: 0), CancellationToken.None);

        Assert.False(result.IsB2CChanged);
        Assert.Equal(0, result.DeltaTowardsOms);
        await inventoryRepository.DidNotReceiveWithAnyArgs().GetAsync(default!, default!, default);
        await inventoryRepository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }

    [Fact(DisplayName = "ConfirmAsync returns a no-change result without patching when AllocatedFromB2BBucketQuantity is less than ShippedQuantity")]
    public async Task ConfirmAsync_InsufficientAllocation_ReturnsNoChangeWithoutPatching()
    {
        var result = await sut.ConfirmAsync(
            CreateRequest(shippedQuantity: 5, allocatedFromB2BBucketQuantity: 4), CancellationToken.None);

        Assert.False(result.IsB2CChanged);
        await inventoryRepository.DidNotReceiveWithAnyArgs().GetAsync(default!, default!, default);
        await inventoryRepository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }

    [Fact(DisplayName = "ConfirmAsync returns a no-change result without throwing when no ItemStockInventory record exists")]
    public async Task ConfirmAsync_MissingInventory_ReturnsNoChangeWithoutThrowing()
    {
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns((ItemStockInventory?)null);

        var result = await sut.ConfirmAsync(CreateRequest(), CancellationToken.None);

        Assert.False(result.IsB2CChanged);
        Assert.Equal(0, result.DeltaTowardsOms);
        await inventoryRepository.DidNotReceiveWithAnyArgs().PatchAsync(default!, default!, default!, default!, default);
    }

    [Theory(DisplayName = "ConfirmAsync applies the correct §4.1 confirmation-type branch")]
    [InlineData(ConfirmationType.PRELIMINARY, 20, 10, 4, 20, 10, 8)]
    [InlineData(ConfirmationType.STANDARD_FOLLOWING_PRELIMINARY, 20, 10, 4, 16, 6, 0)]
    [InlineData(ConfirmationType.STANDARD, 20, 10, 0, 16, 6, 0)]
    public async Task ConfirmAsync_ConfirmationTypeBranches_AppliesExpectedArithmetic(
        ConfirmationType confirmationType,
        int initialB2BAvailable, int initialB2BPrepared, int initialPsc,
        int expectedB2BAvailable, int expectedB2BPrepared, int expectedPsc)
    {
        var aggregate = CreateAggregate("etag-1", initialB2BAvailable, initialB2BPrepared, initialPsc);
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns(aggregate);
        inventoryRepository.PatchAsync(InventoryId, InventoryId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);

        await sut.ConfirmAsync(CreateRequest(confirmationType, shippedQuantity: 4, allocatedFromB2BBucketQuantity: 4), CancellationToken.None);

        Assert.Equal(expectedB2BAvailable, aggregate.B2BAvailable);
        Assert.Equal(expectedB2BPrepared, aggregate.B2BPrepared);
        Assert.Equal(expectedPsc, aggregate.Psc);
    }

    [Fact(DisplayName = "ConfirmAsync patches the aggregate and dispatches domain events on a successful confirmation")]
    public async Task ConfirmAsync_ValidConfirmation_PatchesAggregateAndDispatchesDomainEvents()
    {
        var aggregate = CreateAggregate("etag-1");
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns(aggregate);
        inventoryRepository.PatchAsync(InventoryId, InventoryId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);

        await sut.ConfirmAsync(CreateRequest(), CancellationToken.None);

        await inventoryRepository.Received(1).PatchAsync(
            InventoryId, InventoryId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>());
        await domainEventDispatcher.Received(1).DispatchAsync(Arg.Any<IReadOnlyCollection<IDomainEvent>>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ConfirmAsync invokes B2C extension calculation only when the aggregate IsExtended")]
    public async Task ConfirmAsync_ExtendedAggregate_InvokesExtensionCalculation()
    {
        var aggregate = CreateAggregate("etag-1", isExtended: true);
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns(aggregate);
        inventoryRepository.PatchAsync(InventoryId, InventoryId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);

        await sut.ConfirmAsync(CreateRequest(), CancellationToken.None);

        await extensionCalculationService.Received(1).CalculateB2CExtensionAsync(
            20, aggregate, Arg.Any<ItemStockInventoryDeltaResult>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ConfirmAsync does not invoke B2C extension calculation when the aggregate is not extended")]
    public async Task ConfirmAsync_NonExtendedAggregate_DoesNotInvokeExtensionCalculation()
    {
        var aggregate = CreateAggregate("etag-1", isExtended: false);
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns(aggregate);
        inventoryRepository.PatchAsync(InventoryId, InventoryId, "etag-1", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(aggregate);

        await sut.ConfirmAsync(CreateRequest(), CancellationToken.None);

        await extensionCalculationService.DidNotReceiveWithAnyArgs().CalculateB2CExtensionAsync(
            default, default!, default!, default);
    }

    [Fact(DisplayName = "ConfirmAsync retries against fresh state after a concurrency conflict")]
    public async Task ConfirmAsync_ConcurrencyConflictOnFirstAttempt_RetriesAndSucceeds()
    {
        var staleAggregate = CreateAggregate("stale-etag");
        var freshAggregate = CreateAggregate("fresh-etag");
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>()).Returns(staleAggregate, freshAggregate);
        inventoryRepository.PatchAsync(InventoryId, InventoryId, "stale-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(InventoryId, "stale-etag"));
        inventoryRepository.PatchAsync(InventoryId, InventoryId, "fresh-etag", Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Returns(freshAggregate);

        await sut.ConfirmAsync(CreateRequest(), CancellationToken.None);

        await inventoryRepository.Received(2).GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ConfirmAsync throws ConcurrencyException once retries are exhausted")]
    public async Task ConfirmAsync_ConcurrencyConflictOnEveryAttempt_ThrowsAfterExhaustingRetries()
    {
        inventoryRepository.GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>())
            .Returns(_ => CreateAggregate("etag-x"));
        inventoryRepository.PatchAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<PatchOperation>>(), Arg.Any<CancellationToken>())
            .Throws(new ConcurrencyException(InventoryId, "etag-x"));

        await Assert.ThrowsAsync<ConcurrencyException>(
            () => sut.ConfirmAsync(CreateRequest(), CancellationToken.None));

        await inventoryRepository.Received(3).GetAsync(InventoryId, InventoryId, Arg.Any<CancellationToken>());
    }
}
