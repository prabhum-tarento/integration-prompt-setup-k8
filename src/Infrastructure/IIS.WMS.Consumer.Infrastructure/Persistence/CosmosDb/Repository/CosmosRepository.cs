using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Domain.Common;
using IIS.WMS.Consumer.Domain.Exceptions;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <summary>
/// Generic Cosmos DB repository base (cosmos-db.instructions.md §5-§10). Every CRUD/query/concurrency/
/// pagination rule this service follows lives here once; a per-entity repository only supplies its
/// container name and the <typeparamref name="TDomain"/>/<typeparamref name="TDocument"/> mapping.
/// </summary>
/// <typeparam name="TDomain">Domain aggregate/entity type exposed through the repository interface.</typeparam>
/// <typeparam name="TDocument">
/// Cosmos persistence document shape - must implement <see cref="ICosmosDocument"/> so this base class can
/// log and re-read-on-conflict generically without knowing the concrete document type. Also requires a
/// public parameterless constructor - the selective-column <c>GetAsync(partitionKey, select, ...)</c>
/// overload's column selection builds a new, sparsely-populated instance of this type server-side.
/// </typeparam>
public abstract class CosmosRepository<TDomain, TDocument>
    where TDocument : class, ICosmosDocument, new()
{
    /// <summary>Camel-cases a document's properties for the audit trail's <see cref="AuditEntry.DocumentJson"/>, matching the Cosmos SDK's own <c>CosmosPropertyNamingPolicy.CamelCase</c> (cosmos-db.instructions.md §2) rather than Newtonsoft's PascalCase default.</summary>
    private static readonly JsonSerializerSettings AuditDocumentSerializerSettings = new()
    {
        ContractResolver = new CamelCasePropertyNamesContractResolver(),
    };

    private readonly Container _container;
    private readonly ILogger _logger;
    private readonly ICorrelationContext _correlationContext;
    private readonly IAuditTrailWriter _auditTrailWriter;

    /// <param name="containerName">
    /// The container this repository reads/writes, declared by the derived repository (e.g. a private
    /// const) rather than read from shared configuration - each entity's container name is visible at its
    /// own call site instead of every repository depending on one <c>CosmosDb:ContainerName</c> setting.
    /// Passed as a constructor argument, not a virtual property, so it's available before any derived-class
    /// field initializer would otherwise have run.
    /// </param>
    /// <param name="containerFactory">Resolves and caches the named <see cref="Container"/>.</param>
    /// <param name="logger">Derived repository's own categorized logger (e.g. <c>ILogger&lt;InventoryEventRepository&gt;</c>).</param>
    /// <param name="correlationContext">Read for <see cref="AuditEntry.CorrelationId"/>/<see cref="AuditEntry.Schema"/> on every mutation - see <see cref="EnqueueAudit"/>.</param>
    /// <param name="auditTrailWriter">
    /// Non-blocking sink every mutating method below enqueues an <see cref="AuditEntry"/> onto.
    /// <c>AuditRepository</c> (the <c>AuditLog</c> container's own repository) is constructed with
    /// <see cref="NullAuditTrailWriter.Instance"/> instead of the real singleton, so persisting an audit
    /// record never itself enqueues another one.
    /// </param>
    protected CosmosRepository(
        string containerName,
        ICosmosContainerFactory containerFactory,
        ILogger logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
    {
        _container = containerFactory.GetContainer(containerName);
        _logger = logger;
        _correlationContext = correlationContext;
        _auditTrailWriter = auditTrailWriter;
    }

    /// <summary>Projects a domain instance into its persistence shape for a write.</summary>
    protected abstract TDocument ToDocument(TDomain domain);

    /// <summary>
    /// Rehydrates a domain instance from a document read back from Cosmos. Called with a fully-populated
    /// <typeparamref name="TDocument"/> from every method except the selective-column
    /// <c>GetAsync(partitionKey, select, ...)</c> overload's <c>select</c> path, where
    /// <paramref name="document"/> only has the caller's selected properties populated - every other
    /// property is that type's default. An implementation that assigns every document property straight
    /// into the domain instance (the pattern every mapper in this repo follows) already tolerates this
    /// correctly; it must not additionally validate/throw on a property being absent, since "absent" is
    /// expected there, not a data-integrity problem.
    /// </summary>
    protected abstract TDomain ToDomain(TDocument document);

    /// <summary>Reads a single item by id, or <see langword="null"/> if it doesn't exist.</summary>
    public async Task<TDomain?> GetAsync(string id, string category, CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("Reading item {Id} from partition {Category} in {Container}.", id, category, _container.Id);

        try
        {
            var response = await _container.ReadItemAsync<TDocument>(
                id, new PartitionKey(category), cancellationToken: cancellationToken);

            return ToDomain(response.Resource);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            _logger.LogDebug("Item {Id} not found in partition {Category} in {Container}.", id, category, _container.Id);

            return default;
        }
    }

    /// <summary>Creates a new item. A duplicate create for a deterministic id (redelivery) returns the existing item instead of throwing.</summary>
    public async Task<TDomain> CreateAsync(TDomain entity, CancellationToken cancellationToken = default)
    {
        var document = ToDocument(entity);
        _logger.LogDebug("Creating item {Id} in partition {Category} in {Container}.", document.Id, document.Category, _container.Id);

        try
        {
            var response = await _container.CreateItemAsync(
                document, new PartitionKey(document.Category), cancellationToken: cancellationToken);

            _logger.LogInformation(
                "Created item {Id} in partition {Category} in {Container}, request charge {RequestCharge} RU.",
                document.Id, document.Category, _container.Id, response.RequestCharge);

            EnqueueAudit(AuditOperation.Create, response.Resource.Id, response.Resource.Category, response.Resource);

            return ToDomain(response.Resource);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
        {
            // Redelivered create for a deterministic id (cosmos-db.instructions.md §5) - already
            // applied, so this is a no-op, not a processing failure.
            _logger.LogInformation(
                "Create for {Id} in {Container} conflicted with an existing item - treating as an already-applied redelivery.",
                document.Id, _container.Id);

            return await GetAsync(document.Id, document.Category, cancellationToken)
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
        _logger.LogDebug("Upserting item {Id} in partition {Category} in {Container}.", document.Id, document.Category, _container.Id);

        var response = await _container.UpsertItemAsync(
            document, new PartitionKey(document.Category), cancellationToken: cancellationToken);

        _logger.LogInformation(
            "Upserted item {Id} in partition {Category} in {Container}, request charge {RequestCharge} RU.",
            document.Id, document.Category, _container.Id, response.RequestCharge);

        EnqueueAudit(AuditOperation.Upsert, response.Resource.Id, response.Resource.Category, response.Resource);

        return ToDomain(response.Resource);
    }

    /// <summary>Replaces an existing item, guarded by an ETag match. Throws <see cref="ConcurrencyException"/> on a mismatch.</summary>
    public async Task<TDomain> ReplaceAsync(TDomain entity, string expectedETag, CancellationToken cancellationToken = default)
    {
        var document = ToDocument(entity);
        _logger.LogDebug(
            "Replacing item {Id} in partition {Category} in {Container}, expected ETag {ExpectedETag}.",
            document.Id, document.Category, _container.Id, expectedETag);

        try
        {
            var response = await _container.ReplaceItemAsync(
                document, document.Id, new PartitionKey(document.Category),
                new ItemRequestOptions { IfMatchEtag = expectedETag }, cancellationToken);

            _logger.LogInformation(
                "Replaced item {Id} in partition {Category} in {Container}, request charge {RequestCharge} RU.",
                document.Id, document.Category, _container.Id, response.RequestCharge);

            EnqueueAudit(AuditOperation.Replace, response.Resource.Id, response.Resource.Category, response.Resource);

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
        string id, string category, string expectedETag,
        IReadOnlyList<PatchOperation> operations, CancellationToken cancellationToken = default)
    {
        if (operations.Count > 10)
        {
            throw new ArgumentException(
                "Cosmos DB Patch supports at most 10 operations per request.", nameof(operations));
        }

        _logger.LogDebug(
            "Patching item {Id} in partition {Category} in {Container} with {OperationCount} operation(s), expected ETag {ExpectedETag}.",
            id, category, _container.Id, operations.Count, expectedETag);

        try
        {
            var response = await _container.PatchItemAsync<TDocument>(
                id, new PartitionKey(category), operations,
                new PatchItemRequestOptions { IfMatchEtag = expectedETag }, cancellationToken);

            _logger.LogInformation(
                "Patched item {Id} in partition {Category} in {Container}, request charge {RequestCharge} RU.",
                id, category, _container.Id, response.RequestCharge);

            EnqueueAudit(AuditOperation.Patch, response.Resource.Id, response.Resource.Category, response.Resource);

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
    public async Task DeleteAsync(string id, string category, CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("Deleting item {Id} from partition {Category} in {Container}.", id, category, _container.Id);

        try
        {
            await _container.DeleteItemAsync<TDocument>(
                id, new PartitionKey(category), cancellationToken: cancellationToken);

            _logger.LogInformation("Deleted item {Id} from partition {Category} in {Container}.", id, category, _container.Id);

            EnqueueAudit(AuditOperation.Delete, id, category, document: null);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Already gone - delete is idempotent.
            _logger.LogDebug("Item {Id} was already deleted from partition {Category} in {Container}.", id, category, _container.Id);
        }
    }

    /// <summary>Runs a filtered, sorted, paged query over full items.</summary>
    public async Task<PagedResult<TDomain>> GetPagedAsync(
        QueryOptions<TDomain> options, CancellationToken cancellationToken = default)
    {
        ValidatePartitionScope(options.Category, options.AllowCrossPartitionScan);

        var queryable = BuildFilteredQueryable(
            options.Category, options.PageSize, options.ContinuationToken, options.Predicate, options.OrderBy);

        _logger.LogDebug(
            "Querying items in {Container}, partition {Category}, page size {PageSize}. Query: {Query}",
            _container.Id, options.Category, options.PageSize, queryable);

        var page = await ReadNextPageAsync(queryable, cancellationToken);

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

    /// <summary>Runs a filtered, sorted, paged, projected query - use when only a few fields are needed, to reduce RU cost and payload size.</summary>
    public async Task<PagedResult<TResult>> QueryAsync<TResult>(
        QueryOptions<TDomain, TResult> options, CancellationToken cancellationToken = default)
    {
        ValidatePartitionScope(options.Category, options.AllowCrossPartitionScan);

        var queryable = BuildFilteredQueryable(
            options.Category, options.PageSize, options.ContinuationToken, options.Predicate, options.OrderBy);

        var selector = ExpressionRetargeter.Retarget<TDomain, TDocument, TResult>(options.Selector);
        var projected = queryable.Select(selector);

        _logger.LogDebug(
            "Running projected query on {Container}, partition {Category}, page size {PageSize}. Query: {Query}",
            _container.Id, options.Category, options.PageSize, projected);

        var page = await ReadNextPageAsync(projected, cancellationToken);

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

    /// <summary>
    /// Runs a filtered, sorted, column-limited query scoped to one partition - a lighter-weight
    /// alternative to <see cref="GetPagedAsync"/> when the caller only needs a handful of fields, without
    /// standing up a dedicated projected <c>TResult</c> shape the way <see cref="QueryAsync{TResult}"/>
    /// requires. Unlike <see cref="QueryAsync{TResult}"/>, <paramref name="select"/> still returns
    /// <typeparamref name="TDomain"/> - only the selected properties are populated by Cosmos server-side
    /// (see <see cref="ToDomain"/>'s remarks); every unselected property on each returned instance is that
    /// property's default. <b>Callers must only read properties they explicitly selected.</b> Always scoped
    /// to a single partition - unlike <see cref="GetPagedAsync"/>/<see cref="QueryAsync{TResult}"/>, there
    /// is no cross-partition option here, since every call site this method exists for already knows its
    /// partition key.
    /// </summary>
    /// <param name="category">Partition to scope the query to.</param>
    /// <param name="select">
    /// Properties to fetch, e.g. <c>[x => x.Id, x => x.Category]</c> - omit (or pass <see langword="null"/>/empty)
    /// to fetch every property, same as <see cref="GetPagedAsync"/>. Each selector must be a single simple
    /// property access on <typeparamref name="TDomain"/>.
    /// </param>
    /// <param name="where">Filter applied to the query; <see langword="null"/> means no filter.</param>
    /// <param name="orderBy">Multi-key sort - see <see cref="OrderByClause{T}"/>. <see langword="null"/>/empty means no explicit ordering.</param>
    /// <param name="pageSize">Maximum number of items to return in one page.</param>
    /// <param name="continuationToken">Continuation token from a previous page's <see cref="PagedResult{T}"/>, or <see langword="null"/> to start from the beginning.</param>
    /// <param name="cancellationToken">Token to cancel the query.</param>
    public async Task<PagedResult<TDomain>> GetAsync(
        string category,
        IReadOnlyList<Expression<Func<TDomain, object>>>? select = null,
        Expression<Func<TDomain, bool>>? where = null,
        IReadOnlyList<OrderByClause<TDomain>>? orderBy = null,
        int pageSize = 20,
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
    {
        var queryable = BuildFilteredQueryable(category, pageSize, continuationToken, where, orderBy);

        var projected = select is { Count: > 0 }
            ? queryable.Select(BuildPartialDocumentSelector(select))
            : queryable;

        _logger.LogDebug(
            "Querying selective columns in {Container}, partition {Category}, page size {PageSize}. Query: {Query}",
            _container.Id, category, pageSize, projected);

        var page = await ReadNextPageAsync(projected, cancellationToken);

        _logger.LogInformation(
            "Selective-column query on {Container} returned {Count} item(s), request charge {RequestCharge} RU.",
            _container.Id, page.Count, page.RequestCharge);

        return new PagedResult<TDomain>
        {
            Items = page.Select(ToDomain).ToList(),
            ContinuationToken = page.ContinuationToken,
            Count = page.Count,
        };
    }

    /// <summary>
    /// Reads the next page from a Cosmos LINQ queryable via <see cref="CosmosLinqExtensions.ToFeedIterator{T}"/>.
    /// A <see langword="protected virtual"/> seam solely so an integration-test-only subclass backed by an
    /// in-memory <see cref="ICosmosContainerFactory"/> fake can override it: <c>ToFeedIterator</c> requires the
    /// queryable's provider to be the real Cosmos SDK's internal <c>CosmosLinqQuery&lt;T&gt;</c>, which a plain
    /// in-memory <see cref="IQueryable{T}"/> can never satisfy, so a fake container's query path has nothing
    /// else to intercept. Not configurable via constructor injection - the six CRUD methods above need no such
    /// seam, so this stays a single overridable method rather than a new dependency threaded through every
    /// repository's constructor.
    /// </summary>
    protected virtual async Task<FeedResponse<T>> ReadNextPageAsync<T>(IQueryable<T> queryable, CancellationToken cancellationToken)
    {
        using var iterator = queryable.ToFeedIterator();
        return await iterator.ReadNextAsync(cancellationToken);
    }

    /// <summary>
    /// Builds the base queryable for a page, then applies <paramref name="predicate"/> (if any) and
    /// <paramref name="orderBy"/> - the shared "build + filter + order" prefix behind
    /// <see cref="GetPagedAsync"/>, <see cref="QueryAsync{TResult}"/>, and the selective-column
    /// <c>GetAsync(category, select, where, orderBy, ...)</c> overload. Callers that need
    /// <see cref="ValidatePartitionScope"/> run it before calling this - the selective-column overload
    /// always requires a partition and so never calls it.
    /// </summary>
    private IQueryable<TDocument> BuildFilteredQueryable(
        string? category, int pageSize, string? continuationToken,
        Expression<Func<TDomain, bool>>? predicate, IReadOnlyList<OrderByClause<TDomain>>? orderBy)
    {
        var queryable = CreateBaseQueryable(category, pageSize, continuationToken);

        if (predicate is not null)
        {
            queryable = queryable.Where(ExpressionRetargeter.Retarget<TDomain, TDocument, bool>(predicate));
        }

        return ApplyOrdering(queryable, orderBy);
    }

    /// <summary>Builds the base Cosmos LINQ queryable for a page, scoped to a partition key when one is supplied.</summary>
    private IQueryable<TDocument> CreateBaseQueryable(string? category, int pageSize, string? continuationToken)
    {
        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = category is null ? null : new PartitionKey(category),
            MaxItemCount = pageSize,
        };

        return _container.GetItemLinqQueryable<TDocument>(
            continuationToken: continuationToken, requestOptions: requestOptions);
    }

    /// <summary>
    /// Applies the caller's multi-key sort to the queryable, retargeted onto the persistence document's
    /// properties - the first clause becomes <c>OrderBy</c>/<c>OrderByDescending</c>, every clause after
    /// it chains on as <c>ThenBy</c>/<c>ThenByDescending</c>, so ties left by an earlier key are broken by
    /// the next one in list order.
    /// </summary>
    private static IQueryable<TDocument> ApplyOrdering(
        IQueryable<TDocument> queryable,
        IReadOnlyList<OrderByClause<TDomain>>? orderBy)
    {
        if (orderBy is not { Count: > 0 })
        {
            return queryable;
        }

        IOrderedQueryable<TDocument>? ordered = null;

        foreach (var clause in orderBy)
        {
            var retargeted = ExpressionRetargeter.Retarget<TDomain, TDocument, object>(clause.KeySelector);

            ordered = (ordered, clause.Descending) switch
            {
                (null, false) => queryable.OrderBy(retargeted),
                (null, true) => queryable.OrderByDescending(retargeted),
                (not null, false) => ordered.ThenBy(retargeted),
                (not null, true) => ordered.ThenByDescending(retargeted),
            };
        }

        return ordered!;
    }

    /// <summary>
    /// Builds a projection from <typeparamref name="TDocument"/> to a new, sparsely-populated
    /// <typeparamref name="TDocument"/> carrying only the properties <paramref name="select"/> names -
    /// Cosmos executes this server-side (a SQL <c>SELECT</c> naming only those properties), so unselected
    /// properties never leave the server, unlike fetching the full document and discarding fields
    /// client-side. Requires <typeparamref name="TDocument"/> to have a public parameterless
    /// constructor (the class-level <see langword="new()"/> constraint) - object-initializer-style
    /// construction is exactly what this expression tree builds.
    /// </summary>
    private static Expression<Func<TDocument, TDocument>> BuildPartialDocumentSelector(
        IReadOnlyList<Expression<Func<TDomain, object>>> select)
    {
        var parameter = Expression.Parameter(typeof(TDocument), "x");

        var bindings = select.Select(selector =>
        {
            var member = GetSelectedMember(selector);
            var targetProperty = typeof(TDocument).GetProperty(member.Name)
                ?? throw new InvalidOperationException(
                    $"'{typeof(TDocument).Name}' has no property named '{member.Name}' to select.");

            return Expression.Bind(targetProperty, Expression.Property(parameter, targetProperty));
        });

        var body = Expression.MemberInit(Expression.New(typeof(TDocument)), bindings);

        return Expression.Lambda<Func<TDocument, TDocument>>(body, parameter);
    }

    /// <summary>
    /// Extracts the single property <paramref name="selector"/> accesses (e.g. <c>x => x.Id</c>),
    /// unwrapping the boxing <see cref="UnaryExpression"/> the compiler inserts when a value-type member
    /// is converted to <see cref="object"/> to satisfy <c>Expression&lt;Func&lt;TDomain, object&gt;&gt;</c>.
    /// </summary>
    private static MemberInfo GetSelectedMember(Expression<Func<TDomain, object>> selector)
    {
        var body = selector.Body is UnaryExpression { NodeType: ExpressionType.Convert } unary
            ? unary.Operand
            : selector.Body;

        return body is MemberExpression member
            ? member.Member
            : throw new ArgumentException($"'{selector}' must be a simple property access (e.g. x => x.Id).", nameof(selector));
    }

    /// <summary>Throws if a query would scan across partitions without the caller explicitly allowing it (cosmos-db.instructions.md §6).</summary>
    private static void ValidatePartitionScope(string? category, bool allowCrossPartitionScan)
    {
        if (category is null && !allowCrossPartitionScan)
        {
            throw new ArgumentException(
                "Category is required unless AllowCrossPartitionScan is explicitly set to true.");
        }
    }

    /// <summary>
    /// Enqueues a background, non-blocking audit record for a mutation that already completed
    /// successfully against Cosmos - never called for a redelivered <see cref="CreateAsync"/> that
    /// turned out to be a no-op, or a "not found"/precondition-failed branch where nothing actually
    /// changed. <see cref="IAuditTrailWriter.Enqueue"/> is a synchronous, non-blocking hand-off (see its
    /// own remarks); this method never throws and never delays the caller.
    /// </summary>
    /// <param name="operation">The kind of mutation that just completed.</param>
    /// <param name="entityId">Id of the mutated entity, in its own container.</param>
    /// <param name="entityPartitionKey">Partition key of the mutated entity, in its own container.</param>
    /// <param name="document">The entity's full new-state document, as persisted - <see langword="null"/> for a delete.</param>
    private void EnqueueAudit(AuditOperation operation, string entityId, string entityPartitionKey, TDocument? document)
    {
        var documentJson = document is null
            ? null
            : JsonConvert.SerializeObject(document, AuditDocumentSerializerSettings);

        var entry = AuditEntry.Create(
            id: $"{entityId}:{Guid.NewGuid()}",
            containerName: _container.Id,
            entityId: entityId,
            entityPartitionKey: entityPartitionKey,
            operation: operation,
            correlationId: _correlationContext.CorrelationId,
            schema: _correlationContext.Type,
            documentJson: documentJson,
            timestampUtc: DateTime.UtcNow);

        _auditTrailWriter.Enqueue(entry);
    }
}
