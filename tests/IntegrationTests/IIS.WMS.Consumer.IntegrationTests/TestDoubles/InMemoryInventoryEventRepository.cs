using System.Collections.Concurrent;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Exceptions;
using Microsoft.Azure.Cosmos;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles;

/// <summary>
/// Stands in for the real Cosmos-backed repository in <see cref="CustomWebApplicationFactory"/> so
/// the Api-level integration tests can exercise the real middleware pipeline (routing,
/// versioning, validation, exception handling, correlation id) without a live Cosmos DB - the
/// concurrency/patch/cross-partition behavior this fake does NOT reproduce is covered instead by
/// the Testcontainers-based tests per cosmos-db.instructions.md §13.
/// </summary>
public sealed class InMemoryInventoryEventRepository : IInventoryEventRepository
{
    private readonly ConcurrentDictionary<string, InventoryEvent> items = new();
    private int etagCounter;

    public Task<InventoryEvent?> GetAsync(string id, string category, CancellationToken cancellationToken = default) =>
        Task.FromResult(items.TryGetValue(id, out var item) ? Clone(item) : null);

    public Task<InventoryEvent> CreateAsync(InventoryEvent entity, CancellationToken cancellationToken = default)
    {
        entity.ETag = NextETag();
        var stored = items.GetOrAdd(entity.Id, entity);

        return Task.FromResult(Clone(stored));
    }

    public Task<InventoryEvent> ReplaceAsync(
        InventoryEvent entity, string expectedETag, CancellationToken cancellationToken = default)
    {
        if (!items.TryGetValue(entity.Id, out var current) || current.ETag != expectedETag)
        {
            throw new ConcurrencyException(entity.Id, expectedETag);
        }

        entity.ETag = NextETag();
        items[entity.Id] = entity;

        return Task.FromResult(Clone(entity));
    }

    public Task<InventoryEvent> PatchAsync(
        string id, string category, string expectedETag,
        IReadOnlyList<PatchOperation> operations, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("Not exercised by the Api-level integration tests - see cosmos-db.instructions.md §13 for Patch coverage via Testcontainers.");

    public Task DeleteAsync(string id, string category, CancellationToken cancellationToken = default)
    {
        items.TryRemove(id, out _);

        return Task.CompletedTask;
    }

    public Task<PagedResult<InventoryEvent>> GetPagedAsync(
        QueryOptions<InventoryEvent> options, CancellationToken cancellationToken = default)
    {
        var query = items.Values.AsEnumerable();

        if (options.Predicate is not null)
        {
            query = query.Where(options.Predicate.Compile());
        }

        var result = query.Select(Clone).Take(options.PageSize).ToList();

        return Task.FromResult(new PagedResult<InventoryEvent> { Items = result, Count = result.Count });
    }

    public Task<PagedResult<TResult>> QueryAsync<TResult>(
        QueryOptions<InventoryEvent, TResult> options, CancellationToken cancellationToken = default)
    {
        var query = items.Values.AsEnumerable();

        if (options.Predicate is not null)
        {
            query = query.Where(options.Predicate.Compile());
        }

        var result = query.Select(options.Selector.Compile()).Take(options.PageSize).ToList();

        return Task.FromResult(new PagedResult<TResult> { Items = result, Count = result.Count });
    }

    private string NextETag() => $"etag-{Interlocked.Increment(ref etagCounter)}";

    private static InventoryEvent Clone(InventoryEvent source)
    {
        var clone = InventoryEvent.Rehydrate(
            source.Id, source.WarehouseId, source.Sku, source.OnHandQuantity,
            source.CreatedUtc, source.ModifiedUtc, source.ActiveReservations);
        clone.ETag = source.ETag;

        return clone;
    }
}
