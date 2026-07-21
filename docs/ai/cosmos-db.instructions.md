---
description: 'Azure Cosmos DB data access for this inventory integration service: repository pattern, ETag concurrency, retry, and query rules using the native Microsoft.Azure.Cosmos SDK.'
applyTo: '**/Infrastructure/**/*.cs'
---

# Azure Cosmos DB Data Access

Cosmos DB is this service's primary data store, accessed through the native
`Microsoft.Azure.Cosmos` SDK — not the EF Core Cosmos provider, which lacks
full Patch API and ETag concurrency support that §9 and §10 below depend on.
Solution layering (where this code lives) is defined in
[dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md);
this file only covers Cosmos-specific rules.

**Contents:** 1 [Config & secrets](#1-configuration--secrets) · 2 [Client
registration](#2-cosmosclient-registration--retry-policy) · 3 [Entity
guidelines](#3-domain-entity-guidelines) · 4 [Partition keys](#4-partition-key-design) ·
5 [Repository pattern](#5-repository-pattern) · 6 [Query options &
cross-partition guardrail](#6-query-options--cross-partition-guardrail) ·
7 [Pagination](#7-pagination) · 8 [Filtering, ordering, projection](#8-filtering-ordering-projection) ·
9 [Concurrency & ETag](#9-concurrency--etag-required-reading) · 10 [Patch
operations](#10-patch-operations) · 11 [API boundary rules](#11-api-boundary-rules) ·
12 [DI lifetimes](#12-dependency-injection-lifetimes) · 13 [Testing](#13-testing-requirements) ·
14 [Security](#14-security) · 15 [Performance & cost](#15-performance--cost)

## AI code generation rules (read first)

When generating Cosmos DB code:

1. Use the native `Microsoft.Azure.Cosmos` SDK, async methods only, with
   `CancellationToken` forwarded.
2. Go through the repository abstraction (§5) — never inject `CosmosClient`
   or `Container` into a controller or Application service.
3. Every write that mutates an existing item passes its ETag (§9). If you
   can't produce an ETag for a write path, stop and flag it rather than
   silently doing a last-write-wins replace.
4. Every query supplies a partition key unless the caller explicitly opts
   into a cross-partition scan (§6).
5. Keep documents well under the 2 MB hard limit (§3); don't embed
   unbounded child collections.
6. Generate the matching repository interface method and its test.

## 1. Configuration & Secrets

Cosmos settings are never stored in source code.

```json
{
  "CosmosDb": {
    "AccountEndpoint": "",
    "DatabaseName": "InventoryDb",
    "ContainerName": "InventoryEvents",
    "PartitionKeyPath": "/category"
  }
}
```

* **Local development**: the Cosmos DB Emulator, authenticated with its
  well-known fixed key (never a production key), loaded from user-secrets —
  not `appsettings.json`.
* **Every other environment**: `DefaultAzureCredential` resolving to AKS
  Workload Identity (see
  [kubernetes-deployment-best-practices.instructions.md](kubernetes-deployment-best-practices.instructions.md)).
  There is no account-key configuration entry outside local development —
  if you find one, it's a bug, not a style choice.
* Never commit Cosmos keys, connection strings, or tokens.

## 2. CosmosClient Registration & Retry Policy

Register `CosmosClient` as a singleton, configured for Managed Identity and
RU-throttling retry:

```csharp
builder.Services.AddSingleton(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var env = sp.GetRequiredService<IHostEnvironment>();
    var options = new CosmosClientOptions
    {
        ConsistencyLevel = ConsistencyLevel.Session,
        MaxRetryAttemptsOnRateLimitedRequests = 9,
        MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
        SerializerOptions = new CosmosSerializationOptions
        {
            PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase,
        },
    };

    // Local dev only: the Cosmos DB Emulator's well-known fixed key, read
    // from user-secrets. Every other environment authenticates with
    // DefaultAzureCredential (AKS Workload Identity) — there is no key
    // configuration entry once you leave IsDevelopment().
    return env.IsDevelopment()
        ? new CosmosClient(
            config["CosmosDb:AccountEndpoint"],
            config["CosmosDb:EmulatorKey"], // user-secrets only, never appsettings.json
            options)
        : new CosmosClient(
            config["CosmosDb:AccountEndpoint"],
            new DefaultAzureCredential(),
            options);
});
```

* **Consistency level**: Session (default for this service) — reads within
  a session see their own writes, without paying for Strong consistency's
  latency/cost. Do not lower to Eventual for any path that reads back a
  quantity it just wrote (e.g., a reservation confirmation).
* **`SerializerOptions.PropertyNamingPolicy = CamelCase` is required, not
  optional.** Every entity in §3 is declared with PascalCase C# properties
  (`Category`, `WarehouseId`, …), but §1's config declares
  `PartitionKeyPath: "/category"` — lowercase. The Cosmos SDK's default
  serializer writes the property name exactly as declared unless this
  option is set; without it, `Category` would serialize as `"Category"`
  in the stored JSON and silently miss the container's actual partition key
  path. Setting this once here, on the single registered `CosmosClient`,
  makes every entity's camelCase JSON shape consistent with §1's config
  without needing a per-property `[JsonProperty]` override — the only
  property that keeps an explicit override is `ETag`, because Cosmos's
  system property is `_etag`, a name the naming policy doesn't produce.
* Create the client once; never per-request or inside a controller.
* The retry options above absorb transient `429` throttling on the Cosmos
  call itself. They are deliberately separate from the Polly pipeline in
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md) §3,
  which only covers Service Bus, Blob Storage, and outbound HTTP calls —
  and from the ETag concurrency retry in
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md) §2,
  which handles `412 PreconditionFailed`. Three distinct failure classes,
  three distinct mechanisms; don't collapse them into one retry policy.

Register `Container` as its own singleton, resolved once from the client —
this is what the repository code in §9/§10 injects, not `CosmosClient`
directly:

```csharp
builder.Services.AddSingleton(sp =>
{
    var client = sp.GetRequiredService<CosmosClient>();
    var config = sp.GetRequiredService<IConfiguration>();

    return client.GetContainer(
        config["CosmosDb:DatabaseName"],
        config["CosmosDb:ContainerName"]);
});
```

**Never call `CreateDatabaseIfNotExistsAsync`/`CreateContainerIfNotExistsAsync`
at application startup.** §14 requires the service's identity to hold only
least-privilege *data-plane* RBAC — those calls need *control-plane*
permissions the service's identity deliberately doesn't have. Database and
container creation is an infrastructure-provisioning concern (Bicep/Terraform
in CI/CD), not application startup code; if `GetContainer` is pointed at a
container that doesn't exist yet, that's a deployment ordering bug to fix
in the pipeline, not something the app should paper over by provisioning it
itself.

## 3. Domain Entity Guidelines

Every Cosmos entity:

* Has an `Id` (string) and a partition key property.
* Is JSON-serializable with no circular references.
* Stays well under the **2 MB hard document-size limit** — don't embed
  unbounded child collections; reference large or growing sub-collections
  as separate items instead.
* Carries the ETag Cosmos assigns (see §9) — don't strip it in mapping.
* Has a public parameterless constructor — required by the `new()`
  constraint on `CosmosRepository<TDomain,TDocument>`'s `TDocument`, which
  §8's selective-column `GetAsync` needs to build a sparse instance
  server-side. Every entity here already gets one implicitly (no explicit
  constructor is declared), so this is rarely something to think about.
* Lives in this project's `Persistence/CosmosDb/Entity/` folder (namespace
  `...Persistence.CosmosDb.Entity`) — see §5 for the matching
  `Persistence/CosmosDb/Repository/` convention.

```csharp
public sealed class InventoryEvent
{
    public string Id { get; init; } = default!;
    public string WarehouseId { get; init; } = default!;
    public string Sku { get; init; } = default!;

    // Composite partition key per §4 — kept as its own property (not
    // derived at query time) so every write and query uses the identical
    // value. Populate as $"{WarehouseId}:{Sku}" when constructing the entity.
    public string Category { get; init; } = default!;

    public int OnHandQuantity { get; set; }
    public DateTime CreatedUtc { get; init; }
    public DateTime ModifiedUtc { get; set; }

    [JsonProperty("_etag")]
    public string? ETag { get; init; }
}
```

`Id` is not a random GUID — see §5's `CreateAsync` for why it must be
deterministic.

## 4. Partition Key Design

Partition keys must distribute data evenly, support the service's common
query pattern (lookup by SKU/warehouse), and avoid hot partitions.

* **The property is named `Category`** on every entity in this service
  (`InventoryEvent.Category`, `InventoryBulkImportItem.Category`,
  `OrderArchive.Category`, `AuditEntry.Category`) and is mapped to the
  container's configured `/category` partition path (§1) — one consistent
  name end to end, not a per-entity choice.
* **For `InventoryEvent`/`InventoryBulkImportItem`, the partitioning *value*
  stored in `Category` remains the high-cardinality composite
  `WarehouseId:Sku`, never a true low-cardinality category string** —
  matching the Service Bus `SessionId` convention in
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md) §1 —
  the same key that groups messages for ordering is what partitions the
  data, so one warehouse/SKU's writes are both ordered and co-located. The
  property name `Category` describes its role as *the* partition key
  property, not the shape of the value it holds.
* Avoid a bare `Sku` or bare `WarehouseId` alone for this entity: a single
  large warehouse would otherwise concentrate all of its SKUs' write
  traffic on one logical partition.
* Avoid `Status`, `Country`, or any other genuinely low-cardinality/boolean
  field — these create hot partitions under concurrent inventory updates.

## 5. Repository Pattern

Controllers and Application services never reference `CosmosClient` or
`Container` directly — only the repository interface. `CosmosRepository<TDomain,TDocument>`
and every concrete repository live in this project's
`Persistence/CosmosDb/Repository/` folder (namespace `...Persistence.CosmosDb.Repository`),
mirroring the `Entity/` convention from §3.

Every container name is a `const` on the single `CosmosContainerNames` static
class in `Persistence/CosmosDb/CosmosContainerNames.cs` (namespace
`...Persistence.CosmosDb`) — not scattered as a private `const` inside each
repository, and not read from configuration (§1's `CosmosDb:ContainerName`
setting only feeds the base-container registration in §2, and is not how
per-entity repositories resolve their own container). A concrete repository
passes its constant to the `CosmosRepository<TDomain,TDocument>` base
constructor's `containerName` parameter:

```csharp
public sealed class InventoryEventRepository
    : CosmosRepository<InventoryEvent, InventoryEventDocument>, IInventoryEventRepository
{
    public InventoryEventRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<InventoryEventRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.InventoryEvents, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }
}
```

Adding a new container means adding one `const` to `CosmosContainerNames`
and referencing it from the new repository — never a bare string literal at
the call site.

A mapper's `ToDomain(TDocument document)` must assign every document
property straight into the domain instance without validating or throwing
on an absent one. Every method here hands it a fully-populated document
**except** §8's selective-column `GetAsync`, which hands it a document with
only the caller's selected properties set — the rest at that property's
default. A `ToDomain` that follows the plain-assignment pattern already
tolerates this; one that adds its own non-null checks would break that one
call path.

```csharp
public interface IInventoryEventRepository
{
    Task<InventoryEvent?> GetAsync(
        string id, string category, CancellationToken cancellationToken = default);

    Task<InventoryEvent> CreateAsync(
        InventoryEvent entity, CancellationToken cancellationToken = default);

    Task<InventoryEvent> ReplaceAsync(
        InventoryEvent entity, string expectedETag, CancellationToken cancellationToken = default);

    Task<InventoryEvent> PatchAsync(
        string id, string category, string expectedETag,
        IReadOnlyList<PatchOperation> operations, CancellationToken cancellationToken = default);

    Task DeleteAsync(
        string id, string category, CancellationToken cancellationToken = default);

    Task<PagedResult<InventoryEvent>> GetPagedAsync(
        QueryOptions<InventoryEvent> options, CancellationToken cancellationToken = default);

    Task<PagedResult<TResult>> QueryAsync<TResult>(
        QueryOptions<InventoryEvent, TResult> options, CancellationToken cancellationToken = default);
}
```

Note `expectedETag` is a required parameter on every mutating call, not an
optional one — see §9 for why.

**`CreateAsync` and duplicate delivery.** This service's Cosmos writes are
driven by an at-least-once Kafka → Service Bus pipeline (see
[integration-resiliency.instructions.md](integration-resiliency.instructions.md)),
so a redelivered message can call `CreateAsync` twice for the same logical
event. Make `Id` **deterministic** — derived from the source event (e.g.
the Kafka message key, or a stable `EventId` in the payload), never
`Guid.NewGuid()` — so a duplicate `CreateAsync` call targets the same item
ID both times. Cosmos then rejects the second attempt with `409 Conflict`
(distinct from the `412 PreconditionFailed` concurrency conflict in §9);
the repository implementation catches that specific case and treats it as
"already applied" — return the existing item rather than throwing — so a
redelivered create is a no-op, not a processing failure:

```csharp
try
{
    var response = await container.CreateItemAsync(entity,
        new PartitionKey(entity.Category), cancellationToken: cancellationToken);
    return response.Resource;
}
catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
{
    return await GetAsync(entity.Id, entity.Category, cancellationToken)
        ?? throw new InvalidOperationException(
            $"Create conflicted on id {entity.Id} but the item could not be re-read.");
}
```

### 5a. Exception: repositories split across multiple containers

The default above — one repository, one `containerName` passed to the base
constructor, resolved once — is not universal. `ItemStockInventoryRepository`
is the sanctioned exception: it serves five containers
(`ItemStockInventoryEDC`/`TDC`/`ADC`/`CAECOM`/`BRZ3PL`, one per fulfilment
code) from a single repository instance, because the fulfilment code is only
known per call, not per deployment. For this case, skip the `containerName`
constructor argument and override `CosmosRepository<TDomain,TDocument>`'s
`protected virtual string ResolveContainerName(string? category)` seam
instead, resolving the container name from `category` on every call:

```csharp
protected override string ResolveContainerName(string? category) =>
    category is null
        ? throw new NotSupportedException(
            $"{nameof(ItemStockInventoryRepository)} has no single container to scan across " +
            "fulfilment codes - cross-partition queries are not supported.")
        : CosmosContainerNames.GetItemStockInventoryContainerName(ExtractFulfilmentCode(category));
```

Still add the base container name (or, as here, an allow-listed suffix
enum plus resolver) to `CosmosContainerNames` — never a bare string literal —
and never introduce a lookup table or `switch` for a fixed allow-list where
`Enum.TryParse` on a closed enum does the same job with one line. Throw
`NotSupportedException` (not `ArgumentException`) when `category` is
`null` here: `ValidatePartitionScope` (§6) already owns `ArgumentException`
for "caller forgot a partition key" — a repository with no container to fall
back to for a cross-partition scan is a different failure mode and should
stay distinguishable from it.

**`GetPagedAsync`/`QueryAsync` need their own overload.** The single-argument
`ResolveContainerName(string? category)` above is only safe because
`GetAsync`/`CreateAsync`/`ReplaceAsync`/etc. always pass
`ItemStockInventory.Category` — the full `FulfilmentId:ItemCode:Hallmark:
CountryOfOrigin` composite key — so parsing the fulfilment code out of its
first `:`-delimited segment is reliable. `GetPagedAsync`/`QueryAsync` instead
take `options.Category` off a caller-supplied `QueryOptions<T>`/
`QueryOptions<T,TResult>`, which carries no such guarantee — nothing stops a
caller from passing a bare fulfilment code, an unrelated filter value, or
`null`. A multi-container repository must not parse `options.Category` to
route these two methods; read the routing key from the dedicated
`QueryOptions<T>.FulfilmentCode`/`QueryOptions<T,TResult>.FulfilmentCode`
field instead, via the two-argument
`protected virtual string ResolveContainerName(string? category, string? fulfilmentCode)`
seam (default implementation just defers to the single-argument overload,
so every other repository is unaffected):

```csharp
protected override string ResolveContainerName(string? category, string? fulfilmentCode) =>
    fulfilmentCode is null
        ? throw new NotSupportedException(
            $"{nameof(ItemStockInventoryRepository)} requires {nameof(QueryOptions<>.FulfilmentCode)} " +
            "to route a paged/projected query to the correct container - cross-partition queries are not supported.")
        : CosmosContainerNames.GetItemStockInventoryContainerName(fulfilmentCode);
```

## 6. Query Options & Cross-Partition Guardrail

```csharp
public class QueryOptions<T>
{
    public Expression<Func<T, bool>>? Predicate { get; set; }
    public IReadOnlyList<OrderByClause<T>>? OrderBy { get; set; }
    public int PageSize { get; set; } = 20;
    public string? ContinuationToken { get; set; }
    public string? Category { get; set; }
    public bool AllowCrossPartitionScan { get; set; } = false;
    public string? FulfilmentCode { get; set; }
}
```

`FulfilmentCode` only matters to a multi-container repository (§5a) — every
single-container repository ignores it. It exists so that repository's
`ResolveContainerName(string?, string?)` overload can route
`GetPagedAsync`/`QueryAsync` from an explicit field instead of parsing
`Category`, which is a partition-key filter, not necessarily shaped like the
routing key.

`OrderBy` is a **multi-key** sort, not a single field — each
`OrderByClause<T>` is a `(KeySelector, Descending)` pair, applied in list
order as `OrderBy(...).ThenBy(...)`/`OrderByDescending(...).ThenByDescending(...)`:
the first clause is the primary sort key, every clause after it breaks ties
left by the ones before it. Build the list with the `OrderByClause.Asc<T>(...)`/
`OrderByClause.Desc<T>(...)` factory helpers rather than constructing
`OrderByClause<T>` directly:

```csharp
OrderBy = [OrderByClause.Asc<InventoryEvent>(x => x.Sku), OrderByClause.Desc<InventoryEvent>(x => x.CreatedUtc)]
```

For a projection query (§8) that selects a DTO instead of the full entity,
use the two-generic variant — same filtering/paging/guardrail fields, plus
a required `Selector`:

```csharp
public class QueryOptions<T, TResult>
{
    public Expression<Func<T, bool>>? Predicate { get; set; }
    public Expression<Func<T, TResult>> Selector { get; set; } = default!;
    public IReadOnlyList<OrderByClause<T>>? OrderBy { get; set; }
    public int PageSize { get; set; } = 20;
    public string? ContinuationToken { get; set; }
    public string? Category { get; set; }
    public bool AllowCrossPartitionScan { get; set; } = false;
}
```

The same guardrail check in this section applies to both variants — the
repository's projection query method validates `Category`/
`AllowCrossPartitionScan` exactly like the entity query method does.

The repository implementation **must throw** if `Category` is null and
`AllowCrossPartitionScan` is `false` — this is what makes §7's "minimize
cross-partition queries" rule real instead of aspirational:

```csharp
if (options.Category is null && !options.AllowCrossPartitionScan)
{
    throw new ArgumentException(
        $"{nameof(options.Category)} is required unless " +
        $"{nameof(options.AllowCrossPartitionScan)} is explicitly set to true.",
        nameof(options));
}
```

Put this check at the top of the repository's query method, before
building the Cosmos query — a caller that genuinely needs a
cross-partition scan sets the flag explicitly, which makes the RU-cost
tradeoff a visible decision in the calling code, not an accident.

## 7. Pagination

Large queries always paginate using Cosmos continuation tokens, never
in-memory `Skip`/`Take` over a full result set.

```csharp
public class PagedResult<T>
{
    public IReadOnlyList<T> Items { get; init; } = [];
    public string? ContinuationToken { get; init; }
    public int Count { get; init; }
}
```

## 8. Filtering, Ordering, Projection

Build queries from strongly-typed expressions — never string-concatenate
SQL. When you have the full partition key (`WarehouseId:Sku`), supply it —
this is the common case, a point-ish lookup within one partition:

```csharp
var options = new QueryOptions<InventoryEvent>
{
    Predicate = x => x.Sku == sku && x.WarehouseId == warehouseId,
    OrderBy = [OrderByClause.Desc<InventoryEvent>(x => x.CreatedUtc)],
    Category = $"{warehouseId}:{sku}",
};
```

"List every SKU in a warehouse" filters on `WarehouseId` alone, which is
**not** the full partition key (§4) — this is a genuine cross-partition
scan, so it must set `AllowCrossPartitionScan` explicitly rather than
guessing a partial partition key:

```csharp
var options = new QueryOptions<InventoryEvent>
{
    Predicate = x => x.WarehouseId == warehouseId,
    OrderBy = [OrderByClause.Desc<InventoryEvent>(x => x.CreatedUtc)],
    AllowCrossPartitionScan = true, // querying by WarehouseId alone, not the full WarehouseId:Sku key
};
```

Project to a DTO instead of selecting full documents when only a few fields
are needed — lower RU cost, smaller payload:

```csharp
var options = new QueryOptions<InventoryEvent, InventoryEventSummary>
{
    Selector = x => new InventoryEventSummary { Id = x.Id, Sku = x.Sku },
    Predicate = x => x.Sku == sku && x.WarehouseId == warehouseId,
    Category = $"{warehouseId}:{sku}",
};
```

**Selective-column reads without a dedicated DTO.** `CosmosRepository<TDomain,TDocument>`
also exposes a lighter-weight `GetAsync(category, select:, where:, orderBy:, ...)`
overload for when a caller wants only a handful of fields but doesn't want
to stand up a projected `TResult` shape just for that one call site:

```csharp
var page = await repository.GetAsync(
    category,
    select: [x => x.Id, x => x.Sku],
    where: x => x.Sku == sku,
    orderBy: [OrderByClause.Asc<InventoryEvent>(x => x.Id), OrderByClause.Desc<InventoryEvent>(x => x.CreatedUtc)]);
```

Unlike the projected `QueryAsync<TResult>` above, this still returns
`TDomain` — Cosmos only fetches the named properties server-side, and every
**unselected** property on each returned instance is that property's
default. Callers must only read properties they explicitly named in
`select`. This is why a Cosmos document type needs a public parameterless
constructor (`ICosmosDocument, new()` on the repository base class) and why
a mapper's `ToDomain` must not validate/throw when a property is
absent — see §3 and §5.

## 9. Concurrency & ETag (required reading)

Every entity that supports quantity mutation **must** use ETag-based
optimistic concurrency — this is the mechanism that prevents overselling
under concurrent updates, and every other doc in this repo that mentions
Cosmos concurrency points here.

```csharp
public async Task<InventoryEvent> ReplaceAsync(
    InventoryEvent entity, string expectedETag, CancellationToken cancellationToken = default)
{
    try
    {
        var response = await container.ReplaceItemAsync(
            entity, entity.Id,
            new PartitionKey(entity.Category),
            new ItemRequestOptions { IfMatchEtag = expectedETag },
            cancellationToken);

        return response.Resource;
    }
    catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
    {
        throw new ConcurrencyException(entity.Id, expectedETag);
    }
}
```

* `ConcurrencyException` is caught by the global exception handler in
  [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md) and
  mapped to `409 Conflict`.
* For a message-driven write (Kafka/Service Bus consumer, not an HTTP
  request), a `409`/`PreconditionFailed` means re-read the current item and
  re-apply the operation against the fresh ETag — the bounded
  re-read-and-reapply loop that does this lives in
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md) §2,
  not here. Do not treat it as a fatal error for that message.
* Never implement "last write wins" for any field that represents a
  quantity, reservation, or allocation.

## 10. Patch Operations

Use the Patch API for partial updates instead of a full `ReplaceAsync` —
lower RU cost and a smaller blast radius for concurrent writes.

```csharp
await repository.PatchAsync(
    inventoryEvent.Id,
    inventoryEvent.Category,
    inventoryEvent.ETag!,
    [
        PatchOperation.Increment("/onHandQuantity", -1),
        PatchOperation.Set("/modifiedUtc", DateTime.UtcNow),
    ],
    cancellationToken);
```

* Supported operations: Add, Set, Replace, Remove, Increment.
* **Hard limit: 10 operations per patch request.** The repository
  implementation validates this and throws `ArgumentException` before
  calling Cosmos, rather than letting a `BadRequestException` surface at
  runtime — if a caller needs more than 10 field changes, that's a signal
  the entity should be split or the change modeled as a full replace.
* Always pass `expectedETag` via `PatchItemRequestOptions.IfMatchEtag` —
  the same rule as §9 applies to patches.

Repository implementation — validate the operation count before calling
Cosmos, and pass the ETag the same way `ReplaceAsync` does in §9:

```csharp
public async Task<InventoryEvent> PatchAsync(
    string id, string category, string expectedETag,
    IReadOnlyList<PatchOperation> operations, CancellationToken cancellationToken = default)
{
    if (operations.Count > 10)
    {
        throw new ArgumentException(
            "Cosmos DB Patch supports at most 10 operations per request.",
            nameof(operations));
    }

    try
    {
        var response = await container.PatchItemAsync<InventoryEvent>(
            id,
            new PartitionKey(category),
            operations,
            new PatchItemRequestOptions { IfMatchEtag = expectedETag },
            cancellationToken);

        return response.Resource;
    }
    catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
    {
        throw new ConcurrencyException(id, expectedETag);
    }
}
```

## 11. API Boundary Rules

Controllers and Application services:

* Receive DTOs, validate input, call an Application service.
* Never build Cosmos queries, access `CosmosClient`/`Container`, or contain
  business rules — those live in Domain/Infrastructure per
  [dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md).

## 12. Dependency Injection Lifetimes

| Component | Lifetime |
|---|---|
| `CosmosClient` | Singleton |
| `Container` | Singleton (resolved once from `CosmosClient`, §2) |
| Repository | Scoped |
| Application service | Scoped |

`Container` singletons are cached per container **name**, not per
repository — see §5a's exception: a repository that overrides
`ResolveContainerName` (`ItemStockInventoryRepository`) resolves more than
one cached singleton `Container` over its lifetime, one per distinct name it
requests from `ICosmosContainerFactory`.

## 13. Testing Requirements

* **Unit tests**: mock the repository interface; test Application services
  and Domain rules in isolation.
* **Integration tests**: no Cosmos DB Emulator container image was ever
  successfully wired up for this repo — instead, run against an **in-memory
  `Container` fake** (`InMemoryCosmosContainer`, registered as
  `ICosmosContainerFactory` in place of the real `CosmosContainerFactory` —
  see [integration-resiliency.instructions.md](integration-resiliency.instructions.md)
  §9 for the full pipeline test setup this is part of), covering concurrency
  conflicts (§9) and patch operation limits (§10) with faithful
  `CosmosException`/`HttpStatusCode` semantics (real `412`/`409`/`404`), not
  just enough surface to compile. Cross-partition guardrail behavior (§6) is
  a repository-level check (`ValidatePartitionScope`), not a Cosmos-specific
  one, and needs no container fake to test at all.
* `CosmosRepository`'s two query methods (`GetPagedAsync`/`QueryAsync`) call
  the SDK's own `IQueryable.ToFeedIterator()` extension, which requires a
  genuinely Cosmos-backed queryable — an in-memory fake can never satisfy
  it. `ReadNextPageAsync` is a `protected virtual` seam that exists solely
  for this: its default implementation is exactly today's
  `ToFeedIterator()`/`ReadNextAsync` call, and a test-only repository
  subclass overrides it to materialize the in-memory queryable directly
  instead. The six CRUD methods (`GetAsync`/`CreateAsync`/`UpsertAsync`/
  `ReplaceAsync`/`PatchAsync`/`DeleteAsync`) need no such seam — they call
  `Container` methods the in-memory fake implements directly.

## 14. Security

* `DefaultAzureCredential` / Managed Identity in every non-local
  environment (§1); least-privilege data-plane RBAC role, not the account
  master key, for the service's identity.
* Never log secrets, full document bodies containing PII, or raw Cosmos
  exception messages to the client — see the exception handler in
  [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md).

## 15. Performance & Cost

Optimize partition key design (§4), indexing policy (exclude paths that are
never queried), projection (§8), and pagination (§7). Monitor RU
consumption, `429` rate, and P99 latency per operation — a rising `429`
rate on a specific partition key value is a hot-partition signal, not
something to solve by increasing `MaxRetryAttemptsOnRateLimitedRequests`
further.
