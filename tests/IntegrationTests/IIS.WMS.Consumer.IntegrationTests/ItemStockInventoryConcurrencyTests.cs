using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles.Cosmos;
using Microsoft.Extensions.Logging.Abstractions;

namespace IIS.WMS.Consumer.IntegrationTests;

/// <summary>
/// Forces a real Cosmos <c>412 PreconditionFailed</c> (via <see cref="InMemoryCosmosContainer.ForceNextConflict"/>,
/// integration-resiliency.instructions.md §9) through the actual <see cref="ItemStockInventoryRepository"/> -
/// proving <see cref="CosmosRepository{TDomain,TDocument}.ReplaceAsync"/>'s real
/// <c>CosmosException</c>-to-<c>ConcurrencyException</c> translation and
/// <see cref="ItemStockInventoryService"/>'s re-read-and-reapply retry loop work together end to end, not
/// just against a mocked repository throwing <c>ConcurrencyException</c> directly (see the Application-layer
/// unit tests for that narrower proof). This is the concrete fix for the reported "PreCondition failed"
/// issue this port addresses.
/// </summary>
public sealed class ItemStockInventoryConcurrencyTests
{
    private const string CorrelationId = "corr-1";
    private const string FulfilmentCode = "EDC";
    private static readonly string ContainerName = CosmosContainerNames.GetItemStockInventoryContainerName(FulfilmentCode);

    [Fact(DisplayName = "ApplyPickAsync recovers from a forced Cosmos 412 by re-reading and reapplying against fresh state")]
    public async Task ApplyPickAsync_ForcedPreconditionFailed_RetriesAgainstFreshStateAndSucceeds()
    {
        var factory = new InMemoryCosmosContainerFactory();
        var repository = CreateRepository(factory);
        var aggregate = ItemStockInventory.Rehydrate(
            "EDC:SKU1:925:TH", "EDC", "SKU1", "TH", "925",
            b2bAvailable: 20, b2cAvailable: 20, b2cOriginal: 20, b2cExtended: 0,
            b2cAllocated: 10, b2bAllocated: 10, b2cPrepared: 0, b2bPrepared: 0,
            internalHallmarkAllocated: 0, inTransit: 0, b2cThreshold: 0, isExtended: false, b2bUsedShare: 0,
            inspection: 0, psc: 0, isPosm: false, modifiedUtc: DateTime.UtcNow);
        await repository.CreateAsync(aggregate);

        var container = factory.GetInMemoryContainer(ContainerName)
            ?? throw new InvalidOperationException($"{ContainerName} container was never resolved.");
        container.ForceNextConflict(aggregate.Id);

        var service = new ItemStockInventoryService(
            repository, new NoOpDomainEventDispatcher(), TimeProvider.System,
            NullLogger<ItemStockInventoryService>.Instance);

        await service.ApplyPickAsync("EDC", "SKU1", "TH", "925", ItemStockPickChannel.B2B, 4, CancellationToken.None);

        var afterPick = await repository.GetAsync(aggregate.Id, aggregate.Id, CancellationToken.None);
        Assert.NotNull(afterPick);
        Assert.Equal(6, afterPick!.B2BAllocated);
        Assert.Equal(4, afterPick.B2BPrepared);
    }

    private static ItemStockInventoryRepository CreateRepository(InMemoryCosmosContainerFactory factory)
    {
        var correlationContext = new CorrelationContext();
        correlationContext.Set(CorrelationId, appId: "app-1", types: ["InventoryStateChanged"]);

        return new ItemStockInventoryRepository(
            factory, NullLogger<ItemStockInventoryRepository>.Instance, correlationContext, NullAuditTrailWriter.Instance);
    }

    /// <summary>This test only asserts on the persisted aggregate's post-retry state, not on dispatch - a real dispatcher is exercised by the Application-layer unit tests instead.</summary>
    private sealed class NoOpDomainEventDispatcher : IDomainEventDispatcher
    {
        public Task DispatchAsync(IReadOnlyCollection<IDomainEvent> domainEvents, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
