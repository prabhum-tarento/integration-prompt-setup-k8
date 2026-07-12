using System.Linq.Expressions;
using System.Net;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Exceptions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;

/// <summary>
/// Generic Cosmos DB repository base (cosmos-db.instructions.md §5-§10). Every CRUD/query/concurrency/
/// pagination rule this service follows lives here once; a per-entity repository only supplies its
/// container name and the <typeparamref name="TDomain"/>/<typeparamref name="TDocument"/> mapping.
/// </summary>
/// <typeparam name="TDomain">Domain aggregate/entity type exposed through the repository interface.</typeparam>
/// <typeparam name="TDocument">
/// Cosmos persistence document shape - must implement <see cref="ICosmosDocument"/> so this base class can
/// log and re-read-on-conflict generically without knowing the concrete document type.
/// </typeparam>
public abstract class CosmosRepository<TDomain, TDocument>
    where TDocument : ICosmosDocument
{
    private readonly Container _container;
    private readonly ILogger _logger;

    /// <param name="containerName">
    /// The container this repository reads/writes, declared by the derived repository (e.g. a private
    /// const) rather than read from shared configuration - each entity's container name is visible at its
    /// own call site instead of every repository depending on one <c>CosmosDb:ContainerName</c> setting.
    /// Passed as a constructor argument, not a virtual property, so it's available before any derived-class
    /// field initializer would otherwise have run.
    /// </param>
    /// <param name="containerFactory">Resolves and caches the named <see cref="Container"/>.</param>
    /// <param name="logger">Derived repository's own categorized logger (e.g. <c>ILogger&lt;InventoryEventRepository&gt;</c>).</param>
    protected CosmosRepository(string containerName, ICosmosContainerFactory containerFactory, ILogger logger)
    {
        _container = containerFactory.GetContainer(containerName);
        _logger = logger;
    }

    /// <summary>Projects a domain instance into its persistence shape for a write.</summary>
    protected abstract TDocument ToDocument(TDomain domain);

    /// <summary>Rehydrates a domain instance from a document read back from Cosmos.</summary>
    protected abstract TDomain ToDomain(TDocument document);

    /// <summary>Reads a single item by id, or <see langword="null"/> if it doesn't exist.</summary>
    public async Task<TDomain?> GetAsync(string id, string partitionKey, CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("Reading item {Id} from partition {PartitionKey} in {Container}.", id, partitionKey, _container.Id);

        try
        {
            var response = await _container.ReadItemAsync<TDocument>(
                id, new PartitionKey(partitionKey), cancellationToken: cancellationToken);

            return ToDomain(response.Resource);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            _logger.LogDebug("Item {Id} not found in partition {PartitionKey} in {Container}.", id, partitionKey, _container.Id);

            return default;
        }
    }

    /// <summary>Creates a new item. A duplicate create for a deterministic id (redelivery) returns the existing item instead of throwing.</summary>
    public async Task<TDomain> CreateAsync(TDomain entity, CancellationToken cancellationToken = default)
    {
        var document = ToDocument(entity);
        _logger.LogDebug("Creating item {Id} in partition {PartitionKey} in {Container}.", document.Id, document.PartitionKey, _container.Id);

        try
        {
            var response = await _container.CreateItemAsync(
                document, new PartitionKey(document.PartitionKey), cancellationToken: cancellationToken);

            _logger.LogInformation(
                "Created item {Id} in partition {PartitionKey} in {Container}, request charge {RequestCharge} RU.",
                document.Id, document.PartitionKey, _container.Id, response.RequestCharge);

            return ToDomain(response.Resource);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
        {
            // Redelivered create for a deterministic id (cosmos-db.instructions.md §5) - already
            // applied, so this is a no-op, not a processing failure.
            _logger.LogInformation(
                "Create for {Id} in {Container} conflicted with an existing item - treating as an already-applied redelivery.",
                document.Id, _container.Id);

            return await GetAsync(document.Id, document.PartitionKey, cancellationToken)
                ?? throw new InvalidOperationException(
                    $"Create conflicted on id {document.Id} but the item could not be re-read.");
        }
    }

    /// <summary>
    /// Unconditionally overwrites the item at <paramref name="entity"/>'s partition key - no ETag
    /// check, last write wins. Only correct for data that is not concurrently contested (e.g. an
    /// idempotent bulk-import snapshot reload, per integration-resiliency.instructions.md §1's
    /// bulk-import consumer) - anything requiring the read-modify-write guarantee
    /// <see cref="ReplaceAsync"/>/<see cref="PatchAsync"/> provide must use those instead, not this.
    /// </summary>
    public async Task<TDomain> UpsertAsync(TDomain entity, CancellationToken cancellationToken = default)
    {
        var document = ToDocument(entity);
        _logger.LogDebug("Upserting item {Id} in partition {PartitionKey} in {Container}.", document.Id, document.PartitionKey, _container.Id);

        var response = await _container.UpsertItemAsync(
            document, new PartitionKey(document.PartitionKey), cancellationToken: cancellationToken);

        _logger.LogInformation(
            "Upserted item {Id} in partition {PartitionKey} in {Container}, request charge {RequestCharge} RU.",
            document.Id, document.PartitionKey, _container.Id, response.RequestCharge);

        return ToDomain(response.Resource);
    }

    /// <summary>Replaces an existing item, guarded by an ETag match. Throws <see cref="ConcurrencyException"/> on a mismatch.</summary>
    public async Task<TDomain> ReplaceAsync(TDomain entity, string expectedETag, CancellationToken cancellationToken = default)
    {
        var document = ToDocument(entity);
        _logger.LogDebug(
            "Replacing item {Id} in partition {PartitionKey} in {Container}, expected ETag {ExpectedETag}.",
            document.Id, document.PartitionKey, _container.Id, expectedETag);

        try
        {
            var response = await _container.ReplaceItemAsync(
                document, document.Id, new PartitionKey(document.PartitionKey),
                new ItemRequestOptions { IfMatchEtag = expectedETag }, cancellationToken);

            _logger.LogInformation(
                "Replaced item {Id} in partition {PartitionKey} in {Container}, request charge {RequestCharge} RU.",
                document.Id, document.PartitionKey, _container.Id, response.RequestCharge);

            return ToDomain(response.Resource);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
        {
            _logger.LogWarning(
                "Concurrency conflict replacing item {Id} in {Container}: expected ETag {ExpectedETag} no longer matches the stored item.",
                document.Id, _container.Id, expectedETag);

            throw new ConcurrencyException(document.Id, expectedETag);
        }
    }

    /// <summary>Applies a partial update via the Cosmos Patch API, guarded by an ETag match. At most 10 operations per call.</summary>
    public async Task<TDomain> PatchAsync(
        string id, string partitionKey, string expectedETag,
        IReadOnlyList<PatchOperation> operations, CancellationToken cancellationToken = default)
    {
        if (operations.Count > 10)
        {
            throw new ArgumentException(
                "Cosmos DB Patch supports at most 10 operations per request.", nameof(operations));
        }

        _logger.LogDebug(
            "Patching item {Id} in partition {PartitionKey} in {Container} with {OperationCount} operation(s), expected ETag {ExpectedETag}.",
            id, partitionKey, _container.Id, operations.Count, expectedETag);

        try
        {
            var response = await _container.PatchItemAsync<TDocument>(
                id, new PartitionKey(partitionKey), operations,
                new PatchItemRequestOptions { IfMatchEtag = expectedETag }, cancellationToken);

            _logger.LogInformation(
                "Patched item {Id} in partition {PartitionKey} in {Container}, request charge {RequestCharge} RU.",
                id, partitionKey, _container.Id, response.RequestCharge);

            return ToDomain(response.Resource);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
        {
            _logger.LogWarning(
                "Concurrency conflict patching item {Id} in {Container}: expected ETag {ExpectedETag} no longer matches the stored item.",
                id, _container.Id, expectedETag);

            throw new ConcurrencyException(id, expectedETag);
        }
    }

    /// <summary>Deletes an item. Idempotent - deleting an item that no longer exists is not an error.</summary>
    public async Task DeleteAsync(string id, string partitionKey, CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("Deleting item {Id} from partition {PartitionKey} in {Container}.", id, partitionKey, _container.Id);

        try
        {
            await _container.DeleteItemAsync<TDocument>(
                id, new PartitionKey(partitionKey), cancellationToken: cancellationToken);

            _logger.LogInformation("Deleted item {Id} from partition {PartitionKey} in {Container}.", id, partitionKey, _container.Id);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Already gone - delete is idempotent.
            _logger.LogDebug("Item {Id} was already deleted from partition {PartitionKey} in {Container}.", id, partitionKey, _container.Id);
        }
    }

    /// <summary>Runs a filtered, paged query over full items.</summary>
    public async Task<PagedResult<TDomain>> GetPagedAsync(
        QueryOptions<TDomain> options, CancellationToken cancellationToken = default)
    {
        ValidatePartitionScope(options.PartitionKey, options.AllowCrossPartitionScan);
        _logger.LogDebug(
            "Querying items in {Container}, partition {PartitionKey}, page size {PageSize}.",
            _container.Id, options.PartitionKey, options.PageSize);

        var queryable = CreateBaseQueryable(options.PartitionKey, options.PageSize, options.ContinuationToken);

        if (options.Predicate is not null)
        {
            queryable = queryable.Where(ExpressionRetargeter.Retarget<TDomain, TDocument, bool>(options.Predicate));
        }

        queryable = ApplyOrdering(queryable, options.OrderBy, options.OrderDescending);

        using var iterator = queryable.ToFeedIterator();
        var page = await iterator.ReadNextAsync(cancellationToken);

        _logger.LogInformation(
            "Query on {Container} returned {Count} item(s), request charge {RequestCharge} RU.",
            _container.Id, page.Count, page.RequestCharge);

        return new PagedResult<TDomain>
        {
            Items = page.Select(ToDomain).ToList(),
            ContinuationToken = page.ContinuationToken,
            Count = page.Count,
        };
    }

    /// <summary>Runs a filtered, paged, projected query - use when only a few fields are needed, to reduce RU cost and payload size.</summary>
    public async Task<PagedResult<TResult>> QueryAsync<TResult>(
        QueryOptions<TDomain, TResult> options, CancellationToken cancellationToken = default)
    {
        ValidatePartitionScope(options.PartitionKey, options.AllowCrossPartitionScan);
        _logger.LogDebug(
            "Running projected query on {Container}, partition {PartitionKey}, page size {PageSize}.",
            _container.Id, options.PartitionKey, options.PageSize);

        var queryable = CreateBaseQueryable(options.PartitionKey, options.PageSize, options.ContinuationToken);

        if (options.Predicate is not null)
        {
            queryable = queryable.Where(ExpressionRetargeter.Retarget<TDomain, TDocument, bool>(options.Predicate));
        }

        queryable = ApplyOrdering(queryable, options.OrderBy, options.OrderDescending);

        var selector = ExpressionRetargeter.Retarget<TDomain, TDocument, TResult>(options.Selector);
        var projected = queryable.Select(selector);

        using var iterator = projected.ToFeedIterator();
        var page = await iterator.ReadNextAsync(cancellationToken);

        _logger.LogInformation(
            "Projected query on {Container} returned {Count} item(s), request charge {RequestCharge} RU.",
            _container.Id, page.Count, page.RequestCharge);

        return new PagedResult<TResult>
        {
            Items = page.ToList(),
            ContinuationToken = page.ContinuationToken,
            Count = page.Count,
        };
    }

    /// <summary>Builds the base Cosmos LINQ queryable for a page, scoped to a partition key when one is supplied.</summary>
    private IQueryable<TDocument> CreateBaseQueryable(string? partitionKey, int pageSize, string? continuationToken)
    {
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = partitionKey is null ? null : new PartitionKey(partitionKey),
            MaxItemCount = pageSize,
        };

        return _container.GetItemLinqQueryable<TDocument>(
            continuationToken: continuationToken, requestOptions: requestOptions);
    }

    /// <summary>Applies the caller's ordering expression to the queryable, retargeted onto the persistence document's properties.</summary>
    private static IQueryable<TDocument> ApplyOrdering(
        IQueryable<TDocument> queryable,
        Expression<Func<TDomain, object>>? orderBy,
        bool descending)
    {
        if (orderBy is null)
        {
            return queryable;
        }

        var retargeted = ExpressionRetargeter.Retarget<TDomain, TDocument, object>(orderBy);

        return descending ? queryable.OrderByDescending(retargeted) : queryable.OrderBy(retargeted);
    }

    /// <summary>Throws if a query would scan across partitions without the caller explicitly allowing it (cosmos-db.instructions.md §6).</summary>
    private static void ValidatePartitionScope(string? partitionKey, bool allowCrossPartitionScan)
    {
        if (partitionKey is null && !allowCrossPartitionScan)
        {
            throw new ArgumentException(
                "PartitionKey is required unless AllowCrossPartitionScan is explicitly set to true.");
        }
    }
}
