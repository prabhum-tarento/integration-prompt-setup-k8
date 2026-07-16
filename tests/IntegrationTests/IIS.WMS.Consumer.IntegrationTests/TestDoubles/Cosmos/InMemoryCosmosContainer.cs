using System.Collections.Concurrent;
using System.Net;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles.Cosmos;

/// <summary>
/// In-memory stand-in for a Cosmos <see cref="Container"/> (cosmos-db.instructions.md §13,
/// integration-resiliency.instructions.md §9), backed by a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// of <see cref="JObject"/> keyed by partition key + id. Implements every <see cref="Container"/> member
/// <c>CosmosRepository{TDomain,TDocument}</c> actually calls - <see cref="CreateItemAsync{T}"/>,
/// <see cref="UpsertItemAsync{T}"/>, <see cref="ReplaceItemAsync{T}"/>, <see cref="PatchItemAsync{T}"/>,
/// <see cref="ReadItemAsync{T}"/>, <see cref="DeleteItemAsync{T}"/>, <see cref="GetItemLinqQueryable{T}"/> -
/// with faithful <see cref="CosmosException"/>/<see cref="HttpStatusCode"/> semantics (real ETag conflict
/// checking via <c>IfMatchEtag</c>, real 404/409), not just enough surface to compile. Everything else
/// throws <see cref="NotSupportedException"/> - nothing else is exercised by this repo's code.
/// </summary>
/// <remarks>
/// Documents are stored as <see cref="JObject"/> (Newtonsoft.Json - already a dependency of
/// <c>IIS.WMS.Consumer.Infrastructure</c>, not a new package) serialized with a camelCase contract
/// resolver, matching the property-name convention this repo's <c>PatchOperation</c> paths use (e.g.
/// <c>/onHandQuantity</c>) even though the C# document types themselves are PascalCase - this is what
/// makes <see cref="PatchItemAsync{T}"/>'s path-based operations resolve against the right JSON property.
/// <c>[JsonProperty("_etag")]</c> on each document's <c>ETag</c> property still wins over the resolver
/// (an explicit name always overrides a naming strategy), so round-tripping preserves the real Cosmos
/// system-property name.
/// </remarks>
public sealed class InMemoryCosmosContainer : Container
{
    private static readonly JsonSerializerSettings SerializerSettings = new()
    {
        ContractResolver = new CamelCasePropertyNamesContractResolver(),
    };

    private readonly string containerId;
    private readonly ConcurrentDictionary<string, JObject> store = new();
    private int etagCounter;

    public InMemoryCosmosContainer(string containerId) => this.containerId = containerId;

    public override string Id => containerId;

    public override Database Database => throw new NotSupportedException($"{nameof(InMemoryCosmosContainer)} does not support {nameof(Database)}.");
    public override Conflicts Conflicts => throw new NotSupportedException($"{nameof(InMemoryCosmosContainer)} does not support {nameof(Conflicts)}.");
    public override Scripts Scripts => throw new NotSupportedException($"{nameof(InMemoryCosmosContainer)} does not support {nameof(Scripts)}.");

    /// <summary>Clears every stored item - useful between tests sharing one factory/container instance.</summary>
    public void Clear() => store.Clear();

    private readonly ConcurrentDictionary<string, int> forcedConflicts = new();

    /// <summary>
    /// Test-support hook: makes the next <paramref name="count"/> <see cref="ReplaceItemAsync{T}"/>/
    /// <see cref="PatchItemAsync{T}"/> call(s) for <paramref name="id"/> throw
    /// <see cref="HttpStatusCode.PreconditionFailed"/> regardless of the actual ETag match - lets a test
    /// deterministically exercise the concurrency retry loop
    /// (integration-resiliency.instructions.md §2) without needing genuinely concurrent writers racing
    /// each other.
    /// </summary>
    public void ForceNextConflict(string id, int count = 1) => forcedConflicts[id] = count;

    private bool TryConsumeForcedConflict(string id)
    {
        if (!forcedConflicts.TryGetValue(id, out var remaining) || remaining <= 0)
        {
            return false;
        }

        if (remaining == 1) forcedConflicts.TryRemove(id, out _);
        else forcedConflicts[id] = remaining - 1;

        return true;
    }

    // ------------------------------------------------------------------ //
    // Write operations
    // ------------------------------------------------------------------ //

    public override Task<ItemResponse<T>> CreateItemAsync<T>(
        T item, PartitionKey? partitionKey = null, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default)
    {
        var doc = RequireDocument(item);
        var key = Key(partitionKey ?? new PartitionKey(doc.PartitionKey), doc.Id);
        var jobject = Serialize(item, NextETag());

        if (!store.TryAdd(key, jobject))
        {
            throw NewCosmosException(HttpStatusCode.Conflict, $"Item '{doc.Id}' already exists in container '{containerId}'.");
        }

        return Task.FromResult(BuildItemResponse<T>(jobject, HttpStatusCode.Created));
    }

    public override Task<ItemResponse<T>> UpsertItemAsync<T>(
        T item, PartitionKey? partitionKey = null, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default)
    {
        // No ETag check - last write wins, matching CosmosRepository.UpsertAsync's documented contract.
        var doc = RequireDocument(item);
        var key = Key(partitionKey ?? new PartitionKey(doc.PartitionKey), doc.Id);
        var jobject = Serialize(item, NextETag());
        store[key] = jobject;

        return Task.FromResult(BuildItemResponse<T>(jobject, HttpStatusCode.OK));
    }

    public override Task<ItemResponse<T>> ReplaceItemAsync<T>(
        T item, string id, PartitionKey? partitionKey = null, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default)
    {
        var doc = RequireDocument(item);
        var key = Key(partitionKey ?? new PartitionKey(doc.PartitionKey), id);

        if (!store.TryGetValue(key, out var existing))
        {
            throw NewCosmosException(HttpStatusCode.NotFound, $"Item '{id}' not found in container '{containerId}'.");
        }

        if (TryConsumeForcedConflict(id))
        {
            throw NewCosmosException(HttpStatusCode.PreconditionFailed, $"Forced conflict for item '{id}' (test hook).");
        }

        CheckEtag(existing, requestOptions?.IfMatchEtag, id);

        var jobject = Serialize(item, NextETag());
        store[key] = jobject;

        return Task.FromResult(BuildItemResponse<T>(jobject, HttpStatusCode.OK));
    }

    public override Task<ItemResponse<T>> PatchItemAsync<T>(
        string id, PartitionKey partitionKey, IReadOnlyList<PatchOperation> patchOperations,
        PatchItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default)
    {
        var key = Key(partitionKey, id);

        if (!store.TryGetValue(key, out var existing))
        {
            throw NewCosmosException(HttpStatusCode.NotFound, $"Item '{id}' not found in container '{containerId}'.");
        }

        if (TryConsumeForcedConflict(id))
        {
            throw NewCosmosException(HttpStatusCode.PreconditionFailed, $"Forced conflict for item '{id}' (test hook).");
        }

        CheckEtag(existing, requestOptions?.IfMatchEtag, id);

        var patched = (JObject)existing.DeepClone();
        foreach (var operation in patchOperations)
        {
            ApplyPatchOperation(patched, operation);
        }

        patched["_etag"] = NextETag();
        store[key] = patched;

        return Task.FromResult(BuildItemResponse<T>(patched, HttpStatusCode.OK));
    }

    public override Task<ItemResponse<T>> DeleteItemAsync<T>(
        string id, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default)
    {
        var key = Key(partitionKey, id);

        if (!store.TryRemove(key, out var removed))
        {
            throw NewCosmosException(HttpStatusCode.NotFound, $"Item '{id}' not found in container '{containerId}'.");
        }

        return Task.FromResult(BuildItemResponse<T>(removed, HttpStatusCode.NoContent));
    }

    // ------------------------------------------------------------------ //
    // Read operations
    // ------------------------------------------------------------------ //

    public override Task<ItemResponse<T>> ReadItemAsync<T>(
        string id, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default)
    {
        var key = Key(partitionKey, id);

        if (!store.TryGetValue(key, out var jobject))
        {
            throw NewCosmosException(HttpStatusCode.NotFound, $"Item '{id}' not found in container '{containerId}'.");
        }

        return Task.FromResult(BuildItemResponse<T>(jobject, HttpStatusCode.OK));
    }

    /// <summary>
    /// Builds a plain in-memory <see cref="IOrderedQueryable{T}"/> over every stored item (optionally
    /// scoped to <paramref name="requestOptions"/>'s partition key, capped at <c>MaxItemCount</c>) -
    /// unlike a real Cosmos-backed queryable, this cannot satisfy the SDK's own
    /// <c>IQueryable.ToFeedIterator()</c> extension, which is why
    /// <c>CosmosRepository{TDomain,TDocument}.ReadNextPageAsync{T}</c> exists as an
    /// overridable seam - a test-only repository subclass materializes this queryable directly instead.
    /// Continuation tokens are not supported (always <see langword="null"/>) - the integration tests this
    /// container backs never need to page across more than one page of results.
    /// </summary>
    public override IOrderedQueryable<T> GetItemLinqQueryable<T>(
        bool allowSynchronousQueryExecution = false, string? continuationToken = null,
        QueryRequestOptions? requestOptions = null, CosmosLinqSerializerOptions? linqSerializerOptions = null)
    {
        IEnumerable<JObject> scoped = requestOptions?.PartitionKey is { } partitionKey
            ? store.Where(kvp => kvp.Key.StartsWith(PartitionPrefix(partitionKey), StringComparison.Ordinal)).Select(kvp => kvp.Value)
            : store.Values;

        var items = scoped.Select(Deserialize<T>).AsEnumerable();

        if (requestOptions?.MaxItemCount is > 0 and var maxItemCount)
        {
            items = items.Take(maxItemCount);
        }

        // Dummy ordering to satisfy the IOrderedQueryable<T> return type - CosmosRepository applies its
        // own .OrderBy/.OrderByDescending on top when the caller supplied one.
        return items.AsQueryable().OrderBy(_ => 0);
    }

    // ------------------------------------------------------------------ //
    // Unsupported members - never called by CosmosRepository
    // ------------------------------------------------------------------ //

    public override Task<ContainerResponse> ReadContainerAsync(ContainerRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ResponseMessage> ReadContainerStreamAsync(ContainerRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ContainerResponse> ReplaceContainerAsync(ContainerProperties containerProperties, ContainerRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ResponseMessage> ReplaceContainerStreamAsync(ContainerProperties containerProperties, ContainerRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ContainerResponse> DeleteContainerAsync(ContainerRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ResponseMessage> DeleteContainerStreamAsync(ContainerRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<int?> ReadThroughputAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ThroughputResponse> ReadThroughputAsync(RequestOptions requestOptions, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ThroughputResponse> ReplaceThroughputAsync(int throughput, RequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ThroughputResponse> ReplaceThroughputAsync(ThroughputProperties throughputProperties, RequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ResponseMessage> CreateItemStreamAsync(Stream streamPayload, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ResponseMessage> ReadItemStreamAsync(string id, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ResponseMessage> UpsertItemStreamAsync(Stream streamPayload, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ResponseMessage> ReplaceItemStreamAsync(Stream streamPayload, string id, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ResponseMessage> DeleteItemStreamAsync(string id, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ResponseMessage> PatchItemStreamAsync(string id, PartitionKey partitionKey, IReadOnlyList<PatchOperation> patchOperations, PatchItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<ResponseMessage> ReadManyItemsStreamAsync(IReadOnlyList<(string id, PartitionKey partitionKey)> items, ReadManyRequestOptions? readManyRequestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override Task<FeedResponse<T>> ReadManyItemsAsync<T>(IReadOnlyList<(string id, PartitionKey partitionKey)> items, ReadManyRequestOptions? readManyRequestOptions = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override FeedIterator<T> GetItemQueryIterator<T>(QueryDefinition queryDefinition, string? continuationToken = null, QueryRequestOptions? requestOptions = null) => throw new NotSupportedException();
    public override FeedIterator<T> GetItemQueryIterator<T>(string? queryText = null, string? continuationToken = null, QueryRequestOptions? requestOptions = null) => throw new NotSupportedException();
    public override FeedIterator<T> GetItemQueryIterator<T>(FeedRange feedRange, QueryDefinition queryDefinition, string? continuationToken = null, QueryRequestOptions? requestOptions = null) => throw new NotSupportedException();
    public override FeedIterator GetItemQueryStreamIterator(QueryDefinition queryDefinition, string? continuationToken = null, QueryRequestOptions? requestOptions = null) => throw new NotSupportedException();
    public override FeedIterator GetItemQueryStreamIterator(string? queryText = null, string? continuationToken = null, QueryRequestOptions? requestOptions = null) => throw new NotSupportedException();
    public override FeedIterator GetItemQueryStreamIterator(FeedRange feedRange, QueryDefinition queryDefinition, string? continuationToken = null, QueryRequestOptions? requestOptions = null) => throw new NotSupportedException();
    public override TransactionalBatch CreateTransactionalBatch(PartitionKey partitionKey) => throw new NotSupportedException();
    public override Task<IReadOnlyList<FeedRange>> GetFeedRangesAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public override FeedIterator GetChangeFeedStreamIterator(ChangeFeedStartFrom changeFeedStartFrom, ChangeFeedMode changeFeedMode, ChangeFeedRequestOptions? changeFeedRequestOptions = null) => throw new NotSupportedException();
    public override FeedIterator<T> GetChangeFeedIterator<T>(ChangeFeedStartFrom changeFeedStartFrom, ChangeFeedMode changeFeedMode, ChangeFeedRequestOptions? changeFeedRequestOptions = null) => throw new NotSupportedException();
    public override ChangeFeedEstimator GetChangeFeedEstimator(string processorName, Container leaseContainer) => throw new NotSupportedException();
    public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder<T>(string processorName, ChangesHandler<T> onChangesDelegate) => throw new NotSupportedException();
    public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder<T>(string processorName, ChangeFeedHandler<T> onChangesDelegate) => throw new NotSupportedException();
    public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilderWithManualCheckpoint<T>(string processorName, ChangeFeedHandlerWithManualCheckpoint<T> onChangesDelegate) => throw new NotSupportedException();
    public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder(string processorName, ChangeFeedStreamHandler onChangesDelegate) => throw new NotSupportedException();
    public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilderWithManualCheckpoint(string processorName, ChangeFeedStreamHandlerWithManualCheckpoint onChangesDelegate) => throw new NotSupportedException();
    public override ChangeFeedProcessorBuilder GetChangeFeedEstimatorBuilder(string processorName, ChangesEstimationHandler estimationDelegate, TimeSpan? estimationPeriod = null) => throw new NotSupportedException();

    // ------------------------------------------------------------------ //
    // Private helpers
    // ------------------------------------------------------------------ //

    private static ICosmosDocument RequireDocument<T>(T item) =>
        item as ICosmosDocument
            ?? throw new InvalidOperationException($"{typeof(T).Name} does not implement ICosmosDocument - {nameof(InMemoryCosmosContainer)} only supports documents that do.");

    private string NextETag() => $"etag-{Interlocked.Increment(ref etagCounter)}";

    private static string Key(PartitionKey partitionKey, string id) => $"{partitionKey}|{id}";

    private static string PartitionPrefix(PartitionKey partitionKey) => $"{partitionKey}|";

    private static void CheckEtag(JObject existing, string? expectedETag, string id)
    {
        if (expectedETag is null)
        {
            return;
        }

        var storedETag = existing["_etag"]?.Value<string>();
        if (!string.Equals(storedETag, expectedETag, StringComparison.Ordinal))
        {
            throw NewCosmosException(HttpStatusCode.PreconditionFailed, $"ETag mismatch for item '{id}': expected '{expectedETag}', stored '{storedETag}'.");
        }
    }

    private static CosmosException NewCosmosException(HttpStatusCode statusCode, string message) =>
        new(message, statusCode, subStatusCode: 0, activityId: Guid.NewGuid().ToString(), requestCharge: 0);

    private static JObject Serialize<T>(T item, string etag)
    {
        var jobject = JObject.FromObject(item!, JsonSerializer.Create(SerializerSettings));
        jobject["_etag"] = etag;
        return jobject;
    }

    private static T Deserialize<T>(JObject jobject) => jobject.ToObject<T>(JsonSerializer.Create(SerializerSettings))!;

    private static ItemResponse<T> BuildItemResponse<T>(JObject jobject, HttpStatusCode statusCode) =>
        new InMemoryItemResponse<T>(Deserialize<T>(jobject), statusCode);

    // ------------------------------------------------------------------ //
    // Patch application - Cosmos Patch API path semantics against the camelCase JObject above
    // ------------------------------------------------------------------ //

    private static readonly char[] PathSeparator = ['/'];

    private static string[] SplitPath(string path) => path.Split(PathSeparator, StringSplitOptions.RemoveEmptyEntries);

    private static void ApplyPatchOperation(JObject root, PatchOperation operation)
    {
        switch (operation.OperationType)
        {
            case PatchOperationType.Remove:
                RemoveAtPath(root, operation.Path);
                break;

            case PatchOperationType.Move:
                var moved = SelectAtPath(root, operation.From!) ?? JValue.CreateNull();
                RemoveAtPath(root, operation.From!);
                SetAtPath(root, operation.Path, moved);
                break;

            case PatchOperationType.Increment:
                var current = SelectAtPath(root, operation.Path);
                var incrementValue = GetOperationValue(operation);
                JToken incremented = incrementValue is long or int or short
                    ? new JValue((current?.Type == JTokenType.Integer ? current.Value<long>() : 0L) + Convert.ToInt64(incrementValue))
                    : new JValue((current?.Type is JTokenType.Float or JTokenType.Integer ? current.Value<double>() : 0d) + Convert.ToDouble(incrementValue));
                SetAtPath(root, operation.Path, incremented);
                break;

            case PatchOperationType.Add:
                SetAtPath(root, operation.Path, ToJToken(GetOperationValue(operation)), isAdd: true);
                break;

            case PatchOperationType.Replace:
            case PatchOperationType.Set:
            default:
                SetAtPath(root, operation.Path, ToJToken(GetOperationValue(operation)));
                break;
        }
    }

    /// <summary>Reads the boxed value off a <c>PatchOperation&lt;T&gt;</c> via its publicly exposed <c>Value</c> property.</summary>
    private static object? GetOperationValue(PatchOperation operation) =>
        operation.GetType().GetProperty(nameof(PatchOperation<object>.Value))?.GetValue(operation);

    private static JToken ToJToken(object? value) => value is null ? JValue.CreateNull() : JToken.FromObject(value);

    private static JToken? SelectAtPath(JObject root, string path)
    {
        JToken? current = root;
        foreach (var segment in SplitPath(path))
        {
            current = current switch
            {
                JObject obj => obj[segment],
                JArray arr when int.TryParse(segment, out var index) && index >= 0 && index < arr.Count => arr[index],
                _ => null,
            };
            if (current is null) return null;
        }

        return current;
    }

    private static void SetAtPath(JObject root, string path, JToken value, bool isAdd = false)
    {
        var segments = SplitPath(path);
        JToken current = root;
        for (var i = 0; i < segments.Length - 1; i++)
        {
            var nextSegmentIsArrayIndex = segments[i + 1] == "-" || int.TryParse(segments[i + 1], out _);
            current = current switch
            {
                JObject obj => obj[segments[i]] ??= (nextSegmentIsArrayIndex ? new JArray() : new JObject()),
                JArray arr when int.TryParse(segments[i], out var index) => arr[index],
                _ => throw new InvalidOperationException($"Cannot navigate patch path '{path}'."),
            };
        }

        var lastSegment = segments[^1];
        switch (current)
        {
            case JArray array when lastSegment == "-":
                array.Add(value);
                break;
            case JArray array when int.TryParse(lastSegment, out var index):
                if (isAdd) array.Insert(index, value); else array[index] = value;
                break;
            case JObject jObj:
                jObj[lastSegment] = value;
                break;
            default:
                throw new InvalidOperationException($"Cannot set value at patch path '{path}'.");
        }
    }

    private static void RemoveAtPath(JObject root, string path)
    {
        var segments = SplitPath(path);
        JToken current = root;
        for (var i = 0; i < segments.Length - 1; i++)
        {
            current = current switch
            {
                JObject obj => obj[segments[i]] ?? throw new InvalidOperationException($"Cannot navigate patch path '{path}'."),
                JArray arr when int.TryParse(segments[i], out var index) => arr[index],
                _ => throw new InvalidOperationException($"Cannot navigate patch path '{path}'."),
            };
        }

        var lastSegment = segments[^1];
        switch (current)
        {
            case JObject jObj:
                jObj.Remove(lastSegment);
                break;
            case JArray array when int.TryParse(lastSegment, out var index):
                array.RemoveAt(index);
                break;
        }
    }

    // ------------------------------------------------------------------ //
    // Minimal ItemResponse<T> - only the members CosmosRepository reads
    // ------------------------------------------------------------------ //

    private sealed class InMemoryItemResponse<T>(T resource, HttpStatusCode statusCode) : ItemResponse<T>
    {
        public override T Resource => resource;
        public override HttpStatusCode StatusCode => statusCode;
        public override CosmosDiagnostics Diagnostics => null!;
        public override Headers Headers => new();
        public override double RequestCharge => 0;
        public override string ETag => string.Empty;
    }
}
