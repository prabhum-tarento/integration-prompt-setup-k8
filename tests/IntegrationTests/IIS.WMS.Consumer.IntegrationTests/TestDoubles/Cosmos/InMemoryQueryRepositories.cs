using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles.Cosmos;

/// <summary>
/// Test-only <see cref="InventoryEventRepository"/> subclass overriding
/// <c>CosmosRepository{TDomain,TDocument}.ReadNextPageAsync{T}</c> to materialize the in-memory
/// queryable <see cref="InMemoryCosmosContainer.GetItemLinqQueryable{T}"/> returns directly, instead of
/// calling the real Cosmos SDK's <c>ToFeedIterator()</c> extension (which requires a real Cosmos-backed
/// queryable and cannot be satisfied by any in-memory fake) - see
/// <see cref="InMemoryCosmosContainer"/>'s own remarks. Production behavior is unchanged: this only
/// exists so <see cref="IInventoryEventRepository.GetPagedAsync"/>/<see cref="IInventoryEventRepository.QueryAsync{TResult}"/>
/// work against <see cref="InMemoryCosmosContainerFactory"/> in integration tests
/// (integration-resiliency.instructions.md §9).
/// </summary>
public sealed class InMemoryQueryInventoryEventRepository(ICosmosContainerFactory containerFactory, ILogger<InventoryEventRepository> logger)
    : InventoryEventRepository(containerFactory, logger)
{
    protected override Task<FeedResponse<T>> ReadNextPageAsync<T>(IQueryable<T> queryable, CancellationToken cancellationToken) =>
        Task.FromResult<FeedResponse<T>>(new InMemoryFeedResponse<T>(queryable.ToList()));
}

/// <summary>Same pattern as <see cref="InMemoryQueryInventoryEventRepository"/>, for the bulk-import repository.</summary>
public sealed class InMemoryQueryInventoryBulkImportItemRepository(ICosmosContainerFactory containerFactory, ILogger<InventoryBulkImportItemRepository> logger)
    : InventoryBulkImportItemRepository(containerFactory, logger)
{
    protected override Task<FeedResponse<T>> ReadNextPageAsync<T>(IQueryable<T> queryable, CancellationToken cancellationToken) =>
        Task.FromResult<FeedResponse<T>>(new InMemoryFeedResponse<T>(queryable.ToList()));
}
