---
description: 'Kafka ingestion, Service Bus relay, Blob Storage, Polly resiliency, correlation ID propagation across messaging, async/parallelism/semaphore rules, and integration test setup for this inventory integration service.'
applyTo: '**/Infrastructure/**/*.cs, **/*.Tests/**/*.cs'
---

# Integration & Resiliency

This file owns everything between "a Kafka event arrives" and "the API can
serve it back" — the part of the architecture that doesn't fit cleanly into
the Api layer ([aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md))
or the Cosmos DB repository
([cosmos-db.instructions.md](cosmos-db.instructions.md)). Layering rules
(where this code lives) are in
[dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md) —
all of it lives in `Infrastructure`.

## Data flow

```
Kafka topic (inventory events)
    │  KafkaConsumerHostedService (Confluent.Kafka)
    ▼
Azure Service Bus queue (durable relay)
    │  ServiceBusConsumerHostedService (Azure.Messaging.ServiceBus)
    ▼
Application layer (use case) → Cosmos DB repository (ETag-guarded write)
    │
    ├─▶ Azure Blob Storage — Hot tier: import/export files
    └─▶ Azure Blob Storage — Cold tier: optional raw request/response audit log
```

Both hosted services, and every controller action, share one rule: a
**correlation ID** is attached at the point of entry and carried through
every hop, log line, and outbound call this flow makes.

## 1. Kafka consumer → Service Bus relay

- `KafkaConsumerHostedService : BackgroundService` owns a single long-lived
  `IConsumer<string, string>` (Confluent.Kafka) per hosted-service instance.
  Kafka's consumer-group protocol assigns it a subset of the topic's
  partitions automatically on join/rebalance — the service does not manage
  partition assignment itself and never constructs one consumer object per
  partition.
- On each message: deserialize, resolve or generate the correlation ID (see
  §4), map to the internal event contract, and publish to the Service Bus
  queue — do not write to Cosmos DB directly from the Kafka consumer. The
  Service Bus hop is the durability boundary; skipping it defeats the
  point of the relay.
- Use the Kafka message key (or a stable field from the payload, e.g.
  `WarehouseId:Sku:EventId`) as the outbound Service Bus **message ID** —
  this is what makes the Service Bus consumer's dedupe check (§2) work.
  This ID **must be deterministic across redelivery**: derive it from the
  Kafka partition/offset or an `EventId` already present in the payload,
  never generate a fresh GUID at relay time — a redelivered Kafka message
  that gets a new GUID each attempt defeats the Service Bus consumer's
  dedupe check silently, since every "duplicate" would look new.
- Set the outbound Service Bus message's **`SessionId`** to
  `{WarehouseId}:{Sku}` — the same component order as the Cosmos partition
  key in [cosmos-db.instructions.md](cosmos-db.instructions.md) §4, not a
  coincidence: the key that groups messages for ordering is the same key
  that partitions the data they write to. This groups every message for
  the same inventory aggregate into one Service Bus session — see §2 for
  why that matters for ordering, not just dedup.
- Commit the Kafka offset only after the Service Bus publish succeeds
  (`EnableAutoCommit = false`, manual `Commit` after a confirmed publish).
  A crash between "consumed" and "published" must replay the message, not
  silently drop it.
- **Poison messages on the Kafka side**: a message that fails to
  deserialize will never reach the "publish to Service Bus" step, so the
  offset-after-publish rule above would otherwise replay it forever and
  stall every message behind it on that partition. Deserialization
  failures do not go through the commit-after-publish path — catch them
  explicitly, publish the raw payload plus the error to a
  `inventory-events-dlq` Kafka topic (or, if none exists yet, log at
  `Critical` with the raw payload attached and alert), **then** commit the
  offset so the partition keeps moving. This is the Kafka-side equivalent
  of §2's `DeadLetterMessageAsync` — a message this consumer cannot parse
  is not a transient failure that redelivery will fix.

## 2. Service Bus consumer

The queue has **sessions enabled**, and the consumer uses
`ServiceBusSessionProcessor`, not a plain `ServiceBusProcessor` — this is
what guarantees in-order, single-active-consumer processing for every
message sharing the `SessionId` set in §1 (all events for one
`WarehouseId:Sku`). This turns "two updates to the same SKU racing each
other" from a routine occurrence into a rare cross-boundary case (e.g. a
manual API write landing between two message-driven writes), which is what
makes the ETag retry loop below a defensive backstop rather than the
primary correctness mechanism.

```csharp
var processor = client.CreateSessionProcessor(queueName, new ServiceBusSessionProcessorOptions
{
    MaxConcurrentSessions = 8,        // independent aggregates processed in parallel
    MaxConcurrentCallsPerSession = 1, // one message at a time *within* a session — this is what orders it
    AutoCompleteMessages = false,     // complete/abandon/dead-letter explicitly, only after a durable outcome
});
```

- **Idempotency**: before applying a message, check whether its message ID
  has already been processed (a small dedupe record in Cosmos, or rely on
  the target write being naturally idempotent — see
  [dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md) §5).
  Sessions give you ordering, not exactly-once delivery — redelivery after
  a lock timeout or a crash before `CompleteMessageAsync` is still a normal
  case this code must handle correctly.
- **Concurrency conflict retry (the re-read-and-reapply loop)**: this is
  the one piece of logic every cross-reference to "the concurrency retry
  pattern" in this doc set points to — it lives here, not in Polly (§3),
  because it needs to re-fetch fresh state between attempts, which a blind
  retry of the same delegate cannot do:

```csharp
const int maxAttempts = 3;

for (var attempt = 1; attempt <= maxAttempts; attempt++)
{
    var current = await repository.GetAsync(id, partitionKey, cancellationToken);

    try
    {
        await repository.PatchAsync(id, partitionKey, current!.ETag!, operations, cancellationToken);
        break;
    }
    catch (ConcurrencyException) when (attempt < maxAttempts)
    {
        // Another writer updated this aggregate between our read and our
        // write. Re-fetch (loop continues) and reapply against the new
        // ETag. This is expected to be rare once sessions (above) are in
        // place, so a short bounded retry — not a queue-level redelivery —
        // is the right tool here.
        continue;
    }
    catch (ConcurrencyException)
    {
        // Exhausted retries on what should be a rare conflict — treat as
        // a processing failure for this message, not success.
        throw;
    }
}
```

  A `ConcurrencyException` that escapes this loop is caught by the outer
  message-processing handler below and treated the same as any other
  processing failure (abandon and let Service Bus redeliver, or dead-letter
  once `MaxDeliveryCount` is reached) — it is not retried by the Polly
  pipeline in §3, which only handles transient infrastructure faults
  (Service Bus/Blob/HTTP), not application-level concurrency conflicts.
- On success: `CompleteMessageAsync`. On a transient infrastructure failure
  the Polly policy (§3) already retried and exhausted, or a
  `ConcurrencyException` that exhausted the loop above:
  `AbandonMessageAsync` (goes back to the queue, retried up to
  `MaxDeliveryCount`). On a poison message (fails deserialization, or is
  provably not transient): `DeadLetterMessageAsync` with a reason — never
  silently drop it, and alert on a rising dead-letter count (see
  [kubernetes-deployment-best-practices.instructions.md](kubernetes-deployment-best-practices.instructions.md)).

## 3. Polly resiliency pipeline

Use Polly v8 resilience pipelines for **transient infrastructure faults**
on: Service Bus publish, Blob Storage upload, and any outbound HTTP call to
another service. This pipeline has two things it explicitly does not
cover, each handled elsewhere on purpose:

* **Cosmos RU throttling (`429`)** — handled by the Cosmos SDK's own retry
  options (`MaxRetryAttemptsOnRateLimitedRequests`), configured in
  [cosmos-db.instructions.md](cosmos-db.instructions.md) §2.
* **Cosmos ETag concurrency conflicts (`412`)** — handled by the
  re-read-and-reapply loop in §2 above, because recovering from a
  conflict requires fetching fresh state before retrying, which a blind
  Polly retry of the same delegate can't do.

Don't add `ConcurrencyException` or Cosmos exceptions to this pipeline's
`ShouldHandle` predicate — that would silently retry with stale data
instead of going through the loop that actually re-reads it.

**Each dependency gets its own named pipeline with a predicate matching
*its own* exception type** — Service Bus, Blob Storage, and outbound HTTP
don't throw the same exceptions, so one shared `ShouldHandle` can't cover
all three correctly. Register all three via keyed
`AddResiliencePipeline` and inject by key, rather than copying the
Service Bus example below and pointing it at a different client:

```csharp
services.AddResiliencePipeline("service-bus-publish", builder => builder
    .AddRetry(new RetryStrategyOptions
    {
        ShouldHandle = new PredicateBuilder().Handle<ServiceBusException>(ex => ex.IsTransient),
        MaxRetryAttempts = 5,
        BackoffType = DelayBackoffType.Exponential,
        UseJitter = true,
        Delay = TimeSpan.FromMilliseconds(200),
    })
    .AddCircuitBreaker(new CircuitBreakerStrategyOptions
    {
        FailureRatio = 0.5,
        SamplingDuration = TimeSpan.FromSeconds(30),
        MinimumThroughput = 10,
        BreakDuration = TimeSpan.FromSeconds(15),
    }));

services.AddResiliencePipeline("blob-upload", builder => builder
    .AddRetry(new RetryStrategyOptions
    {
        // Azure.Storage.Blobs throws RequestFailedException; IsTransient
        // isn't exposed on it the way ServiceBusException exposes it, so
        // match on the status codes that are actually retryable.
        ShouldHandle = new PredicateBuilder().Handle<RequestFailedException>(
            ex => ex.Status is 408 or 429 or >= 500),
        MaxRetryAttempts = 5,
        BackoffType = DelayBackoffType.Exponential,
        UseJitter = true,
        Delay = TimeSpan.FromMilliseconds(200),
    }));

services.AddResiliencePipeline<HttpResponseMessage>("outbound-http", builder => builder
    .AddRetry(new RetryStrategyOptions<HttpResponseMessage>
    {
        // A result-aware predicate needs the generic pipeline/options —
        // ShouldHandle here is Func<RetryPredicateArguments<HttpResponseMessage>, ValueTask<bool>>,
        // not the non-generic Outcome<object> shape the Service Bus/Blob
        // pipelines above use. Don't copy this pattern onto a non-generic
        // AddResiliencePipeline call — it won't compile.
        ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
            .Handle<HttpRequestException>()
            .HandleResult(r => (int)r.StatusCode is 408 or 429 or >= 500),
        MaxRetryAttempts = 5,
        BackoffType = DelayBackoffType.Exponential,
        UseJitter = true,
        Delay = TimeSpan.FromMilliseconds(200),
    }));
```

```csharp
// Non-generic pipelines (service-bus-publish, blob-upload):
var pipeline = pipelineProvider.GetPipeline("service-bus-publish");
await pipeline.ExecuteAsync(async ct => await serviceBusSender.SendMessageAsync(message, ct),
    cancellationToken);

// The result-aware outbound-http pipeline needs the generic overload to match
// how it was registered above:
var httpPipeline = pipelineProvider.GetPipeline<HttpResponseMessage>("outbound-http");
var response = await httpPipeline.ExecuteAsync(async ct => await httpClient.GetAsync(url, ct),
    cancellationToken);
```

- Only retry exceptions known to be transient for that specific dependency
  — never blanket-retry every exception, and never retry a validation or
  deserialization failure.
- The circuit breaker (shown on the Service Bus pipeline; add the same
  block to the other two if they show similar failure clustering in
  production) prevents a struggling dependency from being hammered by
  every in-flight message at once; when it's open, fail fast to
  `AbandonMessageAsync` rather than blocking the processor thread.
- Resolve pipelines by key from `ResiliencePipelineProvider<string>`
  (injected via DI) rather than constructing a `ResiliencePipelineBuilder`
  inline per call — this is what makes them configured once, not rebuilt
  per message.

## 4. Correlation ID across messaging

- **HTTP inbound**: generated/read by `CorrelationIdMiddleware` — see
  [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md).
- **Kafka inbound**: if the message carries a `correlationId` header, use
  it; otherwise generate a new GUID at the point of consumption.
- **Kafka → Service Bus**: set the correlation ID as a Service Bus message
  `ApplicationProperties["CorrelationId"]` when relaying (alongside the
  `SessionId` set in §1) — this is the only hop where it needs to move
  from one transport's header shape to another's.
- **Service Bus consumer**: read `ApplicationProperties["CorrelationId"]`
  back into `ICorrelationContext` and the Serilog `LogContext` before
  processing, so every log line for this message — including the eventual
  Cosmos DB write and any Blob upload — carries the same ID an operator can
  grep across all three systems.
- Never regenerate the correlation ID mid-flow once one exists upstream —
  regenerating breaks the one thing this mechanism exists to provide: a
  single ID to trace one event end-to-end.

## 5. Blob Storage

Two containers, two purposes — don't mix them:

- **Hot tier** (`imports`, `exports` containers): import/export files that
  are actively read/written as part of normal operation (e.g., a bulk
  inventory import a warehouse uploads, or an export a downstream consumer
  polls for). Standard hot-tier blob access, `BlobClient`/`BlobContainerClient`,
  behind an interface (`IFileStore`) so callers don't depend on the Azure
  SDK directly.
- **Cold tier** (`request-audit` container): **optional**, feature-flagged
  raw request/response logging for debugging and compliance — enabled per
  environment via configuration, not hardcoded on. When enabled, write with
  `AccessTier.Cold` and name blobs
  `{yyyy}/{MM}/{dd}/{correlationId}.json` so they're both cheap to store
  and directly look-up-able by the correlation ID from a log line.
- Both tiers go through Polly (§3) for transient fault handling.
- Never store a Cosmos account key, connection string, or PII you haven't
  explicitly cleared for audit retention inside a cold-tier blob — treat it
  with the same secrecy rules as
  [engineering-standards.instructions.md](engineering-standards.instructions.md) §6.

## 6. Async, parallelism, and semaphore usage

- Every I/O call is `async`, forwards `CancellationToken`, and follows the
  rules in [csharp.instructions.md](csharp.instructions.md).
- **Bounded fan-out**: when a single Service Bus message expands into
  multiple downstream operations (e.g., a batch import touching many SKUs),
  bound concurrency to the Cosmos RU budget with a `SemaphoreSlim`:

```csharp
using var semaphore = new SemaphoreSlim(initialCount: 8);

var tasks = items.Select(async item =>
{
    await semaphore.WaitAsync(cancellationToken);
    try
    {
        await repository.PatchAsync(item.Id, item.PartitionKey, item.ETag, item.Operations, cancellationToken);
    }
    finally
    {
        semaphore.Release();
    }
});

await Task.WhenAll(tasks);
```

  Prefer `Parallel.ForEachAsync(items, new ParallelOptions { MaxDegreeOfParallelism = 8, CancellationToken = cancellationToken }, ...)`
  when there's no per-item result to collect — it expresses the same bound
  more directly than a hand-rolled semaphore.
- Never launch unbounded `Task.WhenAll` over a collection whose size isn't
  known to be small (a page, not an entire container's contents).
- `ServiceBusSessionProcessorOptions.MaxConcurrentSessions` (§2) and the
  semaphore above bound two different things — how many independent
  aggregates are in flight at once *per pod*, and how much a single message
  fans out within one of those sessions. **The Service Bus consumer is
  horizontally scaled by KEDA** (see
  [kubernetes-deployment-best-practices.instructions.md](kubernetes-deployment-best-practices.instructions.md),
  `maxReplicaCount` on the `servicebus-consumer-scaler` `ScaledObject`) —
  the per-pod bound above is not the whole picture. Size all four numbers
  together so their product stays under the container's provisioned RU/s
  divided by the average RU cost of one write:

  ```
  replicas × MaxConcurrentSessions × semaphore count × avgRuPerWrite ≤ provisionedRUs
  ```

  where `replicas` is the KEDA `ScaledObject`'s `maxReplicaCount` — the
  worst case you must provision for, not the steady-state replica count.
  Start from a conservative estimate (measure `avgRuPerWrite` from the
  Cosmos SDK's `RequestCharge` on a representative patch), then tune down
  `MaxConcurrentSessions`/the semaphore count — not `maxReplicaCount`,
  which exists precisely to absorb load spikes — if the `429` rate
  monitored per [cosmos-db.instructions.md](cosmos-db.instructions.md) §15
  rises at full scale-out, or up if RU utilization stays well below the
  provisioned throughput even with all replicas running under normal load.
  If you change `maxReplicaCount` in the Kubernetes doc, re-check this
  formula — the two numbers are coupled and must be updated together.

## 7. Detailed logging

Every message processed (Kafka consume, Service Bus receive/complete/
abandon/dead-letter, Cosmos write, Blob upload) logs a structured event
with at minimum: `CorrelationId`, `MessageId`, source (`topic:partition:offset`
or `queue:sequenceNumber`), elapsed time, and outcome
(`Succeeded`/`Retried`/`DeadLettered`). Use Serilog's structured
`LogContext` (see
[engineering-standards.instructions.md](engineering-standards.instructions.md) §6) —
never string-interpolate these fields into the message template.

## 8. Health checks contributed by this layer

The Kafka consumer and Service Bus consumer are their own Deployments/Pods
(see
[kubernetes-deployment-best-practices.instructions.md](kubernetes-deployment-best-practices.instructions.md)),
separate from the Api Pod — each hosts its own minimal health endpoint
(`Microsoft.Extensions.Diagnostics.HealthChecks` wired into the worker's
`Host`, exposing `/health/live` and `/health/ready` on a small Kestrel
listener) rather than reporting through the Api's `/health/ready`, which
[aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md) owns
and which cannot observe another Pod's internal state:

- `KafkaConsumerHealthCheck`, on the **Kafka consumer Pod's**
  `/health/ready`: unhealthy if the consumer's last successful poll exceeds
  a configured staleness window (it doesn't need a message to process to
  be healthy — an idle topic isn't a failure).
- `ServiceBusHealthCheck`, on the **Service Bus consumer Pod's**
  `/health/ready`: verifies the management client can reach the namespace.

## 9. Testing this layer

- **Unit tests** (xUnit): mock `IConsumer<,>`, `ServiceBusSender`/
  `ServiceBusProcessor`, and `IFileStore`; test message mapping, dedupe
  logic, and the retry/dead-letter decision tree in isolation. Follow
  `MethodName_Condition_ExpectedResult()` per
  [dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md) §4.
- **Integration tests**: `Testcontainers` spinning up the Kafka broker, the
  Azure Service Bus emulator, the Cosmos DB emulator, and Azurite (Blob
  Storage emulator) — exercise the full
  Kafka → Service Bus → Cosmos DB path against real (containerized)
  dependencies, not mocks, for at least: happy path, duplicate/redelivered
  message, and a forced Cosmos `412 PreconditionFailed` to prove the
  concurrency retry loop in §2 above actually re-reads and re-applies
  against the fresh ETag (the exception itself is raised the way
  [cosmos-db.instructions.md](cosmos-db.instructions.md) §9 shows).
- Coverage target and the shared `WebApplicationFactory<Program>` test host
  setup are defined in
  [engineering-standards.instructions.md](engineering-standards.instructions.md) §7 —
  this file doesn't restate the number.
