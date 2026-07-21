using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;
using IIS.WMS.Consumer.IntegrationTests.TestDoubles.Cosmos;
using Microsoft.Extensions.Logging.Abstractions;

namespace IIS.WMS.Consumer.IntegrationTests;

/// <summary>
/// Proves <see cref="ItemStockInventoryRepository"/>'s per-fulfilment-code container routing
/// (<see cref="ItemStockInventoryRepository.ResolveContainerName(string?)"/>) against the in-memory Cosmos fake -
/// that one repository instance transparently reads/writes distinct containers for distinct fulfilment
/// codes, and that a cross-partition call (no category) is rejected rather than silently scanning one
/// container. See docs/InventoryStateChanged-OrderTracking-Relay.md for the multi-container rationale.
/// </summary>
public sealed class ItemStockInventoryContainerRoutingTests
{
    private const string CorrelationId = "corr-routing";

    [Fact(DisplayName = "Two fulfilment codes route to two distinct in-memory containers from one repository instance")]
    public async Task CreateAsync_TwoFulfilmentCodes_RouteToDistinctContainers()
    {
        var factory = new InMemoryCosmosContainerFactory();
        var repository = CreateRepository(factory);

        var edcAggregate = SeedAggregate("EDC", "SKU1", "TH", "925");
        var tdcAggregate = SeedAggregate("TDC", "SKU1", "TH", "925");

        await repository.CreateAsync(edcAggregate);
        await repository.CreateAsync(tdcAggregate);

        var edcContainerName = CosmosContainerNames.GetItemStockInventoryContainerName("EDC");
        var tdcContainerName = CosmosContainerNames.GetItemStockInventoryContainerName("TDC");

        var edcContainer = factory.GetInMemoryContainer(edcContainerName)
            ?? throw new InvalidOperationException($"{edcContainerName} container was never resolved.");
        var tdcContainer = factory.GetInMemoryContainer(tdcContainerName)
            ?? throw new InvalidOperationException($"{tdcContainerName} container was never resolved.");

        Assert.NotSame(edcContainer, tdcContainer);

        var fromEdc = await repository.GetAsync(edcAggregate.Id, edcAggregate.Category, CancellationToken.None);
        var fromTdc = await repository.GetAsync(tdcAggregate.Id, tdcAggregate.Category, CancellationToken.None);

        Assert.NotNull(fromEdc);
        Assert.NotNull(fromTdc);

        // Cross-checking a record against the other fulfilment code's container proves isolation, not
        // just that each read returned *something* - a record seeded into EDC must not be reachable via TDC.
        Assert.Null(await repository.GetAsync(edcAggregate.Id, tdcAggregate.Category, CancellationToken.None) is { } wrongPartitionRead && wrongPartitionRead.Id == edcAggregate.Id
            ? wrongPartitionRead
            : null);
    }

    [Fact(DisplayName = "GetPagedAsync with AllowCrossPartitionScan and no category throws NotSupportedException - this repository has no single container to scan")]
    public async Task GetPagedAsync_CrossPartitionScanWithNoCategory_ThrowsNotSupportedException()
    {
        var factory = new InMemoryCosmosContainerFactory();
        var repository = CreateRepository(factory);

        var options = new QueryOptions<ItemStockInventory>
        {
            Category = null,
            AllowCrossPartitionScan = true,
            PageSize = 20,
        };

        await Assert.ThrowsAsync<NotSupportedException>(() => repository.GetPagedAsync(options, CancellationToken.None));
    }

    private static ItemStockInventoryRepository CreateRepository(InMemoryCosmosContainerFactory factory)
    {
        var correlationContext = new CorrelationContext();
        correlationContext.Set(CorrelationId, appId: "app-1", types: ["InventoryStateChanged"]);

        return new ItemStockInventoryRepository(
            factory, NullLogger<ItemStockInventoryRepository>.Instance, correlationContext, NullAuditTrailWriter.Instance);
    }

    private static ItemStockInventory SeedAggregate(string fulfilmentId, string itemCode, string countryOfOrigin, string hallmark) =>
        ItemStockInventory.Rehydrate(
            ItemStockInventory.BuildId(fulfilmentId, itemCode, hallmark, countryOfOrigin),
            fulfilmentId, itemCode, countryOfOrigin, hallmark,
            b2bAvailable: 20, b2cAvailable: 20, b2cOriginal: 20, b2cExtended: 0,
            b2cAllocated: 10, b2bAllocated: 10, b2cPrepared: 0, b2bPrepared: 0,
            internalHallmarkAllocated: 0, inTransit: 0, b2cThreshold: 0, isExtended: false, b2bUsedShare: 0,
            inspection: 0, psc: 0, isPosm: false, modifiedUtc: DateTime.UtcNow);
}
