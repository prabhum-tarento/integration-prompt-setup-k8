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
    │  ConsumerHostedService subclasses (Confluent.Kafka) - one per consumer,
    │  each registering one or more schemas: KafkaConsumerHostedService (JSON),
    │  InventoryStateChangedConsumerHostedService (Avro), BulkInventoryImportConsumerHostedService (Avro)
    ▼
Azure Service Bus queue (durable relay)
    │  ServiceBusConsumerHostedService (session-enabled) or
    │  BulkImportServiceBusConsumerHostedService (non-session) (Azure.Messaging.ServiceBus)
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

- Every Kafka consumer is a concrete subclass of the shared
  `ConsumerHostedService : BackgroundService` (no longer generic over a
  single `TValue`), which owns a single long-lived `IConsumer<string, byte[]>`
  (Confluent.Kafka) per hosted-service instance — Kafka's consumer-group
  protocol assigns it a subset of the topic's partitions automatically on
  join/rebalance, so the service does not manage partition assignment
  itself and never constructs one consumer object per partition. **A
  single topic/consumer group can carry more than one schema/event type**:
  a concrete consumer registers one `ISchemaHandler` per schema it
  understands (built via the base class's `CreateSchemaHandler<TValue>`,
  which type-erases each schema's own `IDeserializer<TValue>`,
  JSON-serializer, Service Bus router, and validator behind a common
  non-generic interface), keyed by the Kafka `Type` header value
  (`WellKnownHeaderNames.Type`) that schema corresponds to. A consumer that
  handles exactly one schema regardless of that header's value registers
  its one handler under `ConsumerHostedService.DefaultEventType` instead of
  an exact value; the header's actual value is looked up first, falling
  back to that default key, and a value matching neither is dead-lettered
  (raw bytes, hot-tier) as an unrecognized schema rather than attempting to
  deserialize it. See `KafkaConsumerHostedService` (JSON contract,
  `DefaultEventType`) and `BulkInventoryImportConsumerHostedService` (Avro,
  high-volume, non-session relay, `DefaultEventType`) for that single-schema
  case. `InventoryStateChangedConsumerHostedService` is the one exception:
  it relays two structurally unrelated Avro event types sharing the
  `inventory-events` topic (`InventoryStateChanged` and `InventoryAdjusted`),
  so it registers one handler per exact `Type` header value instead of
  `DefaultEventType`, sharing a single Schema Registry client between them.
  Each consumer can be turned off independently via its own `Enabled`
  configuration key (checked by the base class at startup) without removing
  its configuration section.
- **Broker connection security** - `ConsumerOptions.Protocol` (`SecurityProtocol`:
  `Plaintext`/`Ssl`/`SaslPlaintext`/`SaslSsl`), `AuthenticationMode`
  (`SaslMechanism`: `Plain`/`ScramSha256`/`ScramSha512`/`OAuthBearer`/`Gssapi`),
  `Username`, and `Password` map directly onto the equivalently-named
  `Confluent.Kafka.ConsumerConfig` properties, event-level-first/Kafka-level-
  fallback like `BootstrapServers`. Left unset, Confluent.Kafka's own default
  (`Plaintext`, no SASL) applies - correct for the local emulator, never for
  a real cluster. `Username`/`Password` are secrets: never set in
  `appsettings.json`, local development reads them from user-secrets, every
  other environment from Azure Key Vault, per
  engineering-standards.instructions.md §6 - same rule as every other
  credential in this repo. An inconsistent combination (a `SaslMechanism`
  set without a SASL `Protocol`, say) is rejected by Confluent.Kafka itself
  when the consumer is built; this repo doesn't re-validate that.
- **`EnableAutoCommit`/`AutoOffsetReset`** are also topic (event-level)
  configurable, Kafka-level-fallback like everything else above, but unlike
  `Protocol`/`Username`/etc. the Kafka level always pins an explicit value
  (`false`/`Earliest`) rather than leaving these two genuinely unset - see
  `ConsumerOptions.EnableAutoCommit`'s remarks for why: Confluent.Kafka's own
  defaults if left null are `true`/`Latest`, the opposite of what this
  service's `PartitionOffsetCommitTracker`-driven manual-commit architecture
  requires. **`EnableAutoCommit: true` is correctness-sensitive, not a style
  preference** - it hands offset advancement back to librdkafka's own timer,
  which can commit an offset before this service has actually finished (or
  dead-lettered) the corresponding message, so a crash in that window
  silently skips the message on restart instead of redelivering it. A topic
  that legitimately wants to override the Kafka-level fallback is more often
  reaching for `AutoOffsetReset: Latest` (e.g. a fresh consumer group that
  should skip historical backlog, like `BulkInventoryImport` in
  `appsettings.Development.json`) than for `EnableAutoCommit`.
- On each message, in order: read the Kafka headers (correlation id, dedup
  id, event type, app id — see §4 for the exact header names) and log them;
  check the correlation id against the configured ignore-list
  (`ConsumerOptions.IgnoreCorrelationIdPrefixes`/`IgnoreCorrelationIdSuffixes`,
  falling back Kafka-level → event-level the same way `Enabled`/`WorkerCount`
  do — a match on either list, checked case-insensitively against the raw
  `Correlation-Id` header, skips the message entirely: not deserialized, not
  audit-logged, not deduplicated, not published, just logged at
  `Information` and its offset committed forward, the same treatment as
  `ValidateAsync` returning `false` below. Both lists unset/empty (the
  default) accepts every message, and a message with no `Correlation-Id`
  header at all is always accepted regardless of configuration — there's
  nothing to match a prefix/suffix against); resolve the schema handler for
  this message's event type (falling back to
  `DefaultEventType`, dead-lettering raw bytes if neither matches);
  deserialize; write the deserialized value as JSON to the cold-tier audit
  container (`request-audit`, see §5) unconditionally; check the message
  against the Nexus deduplication service (`IDeduplicationService`, keyed
  on the `Deduplication-Id` header — a duplicate is logged and its offset
  committed, without validating or publishing it; a per-topic
  `DeduplicationCheckEnabled` setting, falling back Kafka-level → event-level
  the same way `Enabled`/`WorkerCount`/`ChannelCapacity` do, can skip this
  check entirely for a topic where Nexus coverage doesn't apply); run the
  resolved handler's `ValidateAsync` (always valid by default — a schema
  registers its own validator, e.g. a FluentValidation `IValidator<TValue>`,
  via `CreateSchemaHandler`), then — only if that passed — the second,
  **dynamic validation** stage; route (`GetServiceBusRouting`) and publish to
  the Service Bus queue — do not write to Cosmos DB directly from the Kafka
  consumer. Both `ValidateAsync` calls (schema and dynamic) share the same
  contract: `bool`, not just throw-to-fail: a
  **throw** is a hard failure (malformed/invalid data, handled the same as
  every other failure mode below), while returning **`false`** means the
  message is valid but this consumer deliberately chooses not to relay it —
  logged, offset committed forward, but **not** written to hot storage,
  since nothing failed. Deserialization happens *before* the cold-tier log
  and dedup check here — the cold-tier record is of the deserialized value,
  not the raw bytes — via the consumer's own raw `byte[]` read
  (Confluent.Kafka's built-in `byte[]` deserializer, which never throws)
  and the resolved handler's deserializer (JSON or Avro), invoked manually
  on every message. **Known trade-off**: any field a newer producer's schema
  adds that this consumer's currently-deployed reader schema doesn't
  recognize is silently absent from the cold-tier audit trail, since it
  never survives the deserialize step — a raw-bytes record for the same
  message only exists if deserialization itself fails (see below).
- **Dynamic validation** (the second `ValidateAsync` stage above): a
  blob-stored, Roslyn-scripted per-`{Transport}/{Identifier}.cs` template via
  `IIS.WMS.Common.DynamicValidation.IDynamicEventValidator` — for this Kafka
  consumer, transport is the fixed folder `Kafka` and identifier is the
  message's Kafka `Type` header value, e.g. `Kafka/inventory.InventoryStateChanged.cs`
  (no template stored for a given identifier means the message passes this
  stage untouched). This runtime (compiler, cache, script-globals contract) lives
  in the shared kernel `IIS.WMS.Common`, not in this Infrastructure project,
  because it's shared across transports — the Kafka consumer here, the
  session-enabled `ServiceBusConsumerHostedService` (§2), and eventually a
  Producer project's own Service Bus hosted service. Each transport adapts
  its own native header type into the transport-neutral
  `IIS.WMS.Common.Messaging.HeaderLookup` before calling `ValidateAsync` — a
  stored template reads headers through `HeaderLookup`/`TryGetHeader`, never
  a transport SDK type directly. A template that needs to resolve a
  Consumer-specific service (e.g. `IDeduplicationService`) can, because
  `ValidationScriptCompiler`'s `ScriptOptions` are extended per-transport via
  `IValidationScriptReferenceProvider` (each transport registers its own
  implementation supplying the assemblies/imports its templates need) rather
  than the compiler hardcoding a Consumer assembly reference — this is what
  keeps `IIS.WMS.Common` at zero `ProjectReference`s despite hosting the
  compiler.
- **No single message's failure stops the consumer or blocks any other
  message** — every failure mode below is handled uniformly, and this is a
  deliberate design choice, not just a poison-message backstop: multiple
  upstream systems produce onto the same topics on their own release
  cadence, so a producer running a newer version of a shared Avro schema
  than this consumer's currently-deployed reader is an expected, recurring
  event, and Service Bus itself can degrade independently of that. Every
  failure is logged at `Critical` tagged with which stage failed, and the
  offset is committed forward through `PartitionOffsetCommitTracker` (not
  a direct `consumer.Commit`) so the rest of the partition keeps flowing:
  - **An event type with no registered schema handler** (exact match or
    `DefaultEventType`): the **raw message bytes** are written to the
    hot-tier `consumer-dead-letter` container (see §5) — there is no
    deserializer to even attempt.
  - **Deserialize** (or the JSON serialize step immediately after it)
    fails: the **raw message bytes** are written to the hot-tier
    `consumer-dead-letter` container (see §5) — there is no successfully
    deserialized value to serialize as JSON at this point.
  - **Validation** (`ValidateAsync` throws — not the same as it returning
    `false`, see above) or **Service Bus publish** fails, even after
    `ResiliencePipelines.ServiceBusPublish`'s retries are exhausted: the
    deserialized value, as **JSON**, is written to the hot-tier container
    instead — a valid value already exists by then.
  A separate, out-of-band watcher process reprocesses whatever lands in
  the hot-tier container once the underlying issue (schema mismatch, bad
  data, transient Service Bus problem) is resolved. **Accepted trade-offs**
  of treating a Service Bus publish failure as non-fatal instead of
  faulting the worker and restarting the pod (this design's behavior
  before this section was last revised): a genuine, sustained Service Bus
  outage now dead-letters every in-flight message for its duration instead
  of surfacing as an outage that Kafka redelivery would otherwise have
  recovered from automatically, in order, once Service Bus recovered; and
  because Service Bus sessions (`{WarehouseId}:{Sku}`, below) provide
  per-aggregate ordering, a message that fails to publish while a *later*
  message for the same aggregate succeeds leaves that session's ordering
  broken until the watcher replays the failed one. Accepted deliberately in
  exchange for "one bad event never blocks the partition" applying
  uniformly to every stage, not only deserialize/validate.
- The deduplication check itself **fails open**: if the call to the Nexus
  deduplication service throws (e.g. Nexus is unavailable), the message is
  logged at `Warning` and treated as *not* a duplicate, rather than as
  fatal or as a confirmed duplicate — either alternative would be worse (a
  Nexus outage dead-lettering the entire topic, or silently dropping every
  message while Nexus is down). This relies on the downstream Service Bus
  consumer's own idempotency check (§2) as the backstop; this consumer's
  dedup check is a latency/cost optimization on top of that, not the only
  defense against a redelivered message being applied twice.
- **Per-message timing, logged individually on success**: every stage of
  `ProcessMessageAsync` is measured with its own `Stopwatch` and logged
  together on the structured "relayed" log line — not just a single combined
  number — so a regression in one stage (e.g. validation getting slower
  after a new rule is added) is visible without correlating separate log
  lines: `DeserializeDurationMs` (includes the Avro→DTO mapping step for a
  schema registered via `CreateSchemaHandler<TAvro,TValue>` — mapping isn't
  separately measurable, since it runs inside the same `ISchemaHandler.Deserialize`
  call), `ColdAuditBlobWriteDurationMs`, `DeduplicationDurationMs` (measured
  here regardless of outcome, including the fail-open path — the dedup
  service's own implementation, e.g. `NexusDeduplicationService`, separately
  logs its own per-call debug line, which this duration does not duplicate),
  `SchemaValidationDurationMs`, `DynamicValidationDurationMs`,
  `OrderArchiveDurationMs`, `BlobOffloadDurationMs` (zero unless the claim-check
  step below actually offloaded), `ServiceBusPublishDurationMs`, and
  `TotalDurationMs`.
- **Claim-check offload for oversized payloads**: `ConsumerOptions.MaxServiceBusMessageSizeBytes`
  (event-level-first/Kafka-level-fallback like every other per-topic setting
  in this section, bottoming out at `ConsumerOptions.DefaultMaxServiceBusMessageSizeBytes`)
  caps how large the schema's JSON payload can be before it's carried inline
  in `ServiceBusRelayEnvelope.ReflexSchema`. A payload over that threshold is
  uploaded instead to the hot-tier `BlobStorageOptions.LargePayloadContainerName`
  container, `ReflexSchema` is left empty, and `ServiceBusRelayEnvelope.BlobPath`
  carries the blob's path - keeping the outbound Service Bus message under
  the broker's own per-message size limit regardless of source payload size.
  A blob upload failure here is handled the same as every other non-fatal
  failure mode above (logged `Critical`, JSON to hot storage, offset
  committed forward). **Producer side only** - `ServiceBusConsumerHostedService`
  does not yet rehydrate a `BlobPath`-offloaded payload back on the consume
  side; see that class's own remarks for the open TODO.
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
- **`ServiceBusSender` lifecycle.** `ConsumerHostedService` caches one
  `ServiceBusSender` per distinct queue name actually used (keyed off
  `ISchemaHandler.ServiceBusQueueName`/`ConsumerOptions.ServiceBusQueueName`,
  not per schema), reused for the lifetime of the app per Microsoft's
  Service Bus client-lifetime guidance — never opened per message. Since
  `ConsumerHostedService` is itself registered `AddSingleton`, this cache
  lives for the process's lifetime. It implements `IAsyncDisposable`
  alongside `IDisposable` so every cached sender is disposed on a graceful
  shutdown: the default `ServiceProvider` prefers `DisposeAsync` over
  `Dispose` when a singleton implements both, and that runs during
  `IHost.StopAsync` — the window a Kubernetes-initiated pod termination
  (redeploy, scale-down) opens with SIGTERM before `terminationGracePeriodSeconds`
  elapses and SIGKILL follows
  (kubernetes-deployment-best-practices.instructions.md). A hard
  crash/SIGKILL/OOM-kill runs no disposal code at all — by design, not a gap
  to close — the broker reclaims that connection's AMQP links via its own
  idle-connection timeout, and a fresh process opens fresh senders on
  restart. An admin endpoint (`GET`/`DELETE /api/v{version}/service-bus-senders`,
  `IServiceBusSenderCacheService`) lists and force-clears every registered
  consumer's cached senders on demand — scoped to **whichever pod handles
  the request**, not every replica: no cross-pod broadcast mechanism exists
  in this repo, and building one was explicitly out of scope when this
  endpoint was added. This also only covers consumers running in the same
  process as the Api — once the target 3-Deployment split below separates
  the Kafka consumer into its own Pod, this service needs to move there too,
  the same way each Pod's own `/health/ready` already only reports on that
  process's own dependencies.
- **Single poll loop, many concurrent workers.** `Consume`/`Commit` on one
  `IConsumer` are meant to be driven from a single thread, so the poll loop
  itself stays single-threaded — but polling is decoupled from the Service
  Bus publish step by a bounded `System.Threading.Channels.Channel`
  between them: the poll loop writes each consumed message to the channel,
  and `ConsumerOptions.WorkerCount` concurrent workers drain it and
  publish. This is what lets one consumer instance sustain a
  high-throughput topic (tens of thousands of messages/second) without
  needing an equally large number of Kafka partitions to get equivalent
  parallelism — see §6 for when this pattern is worth reaching for over
  more partitions/pods, and how to size `WorkerCount`/`ChannelCapacity`.
- **Commit the Kafka offset only once this message's flow has reached a
  terminal outcome** (`EnableAutoCommit = false`, manual `Commit`) — a
  successful Service Bus publish, a confirmed duplicate, or one of the
  non-fatal failure outcomes above (each of which already durably persisted
  the message to the hot-tier container first). A crash mid-flight, before
  any of those, must still replay the message on restart, not silently drop
  it — this is why the offset is committed only once the message's outcome
  (success or hot-tier dead-letter) is durable, never speculatively before.
  With multiple concurrent workers, "committing" is no longer just "commit
  this one message's offset": workers can finish out of order, so a later
  offset can complete before an earlier one on the same partition.
  `PartitionOffsetCommitTracker` (used by
  `ConsumerHostedService`) tracks each partition's contiguous
  low-water mark and only commits up to the highest offset that's safe —
  i.e. every offset below it has actually completed — never a bare
  `consumer.Commit(result)` per message once more than one worker is in
  play. **Known limitation**: this does not handle a partition being
  revoked and reassigned mid-flight (no
  `SetPartitionsRevokedHandler`/`SetPartitionsAssignedHandler` yet) — a
  rebalance while messages are in flight can hand a duplicate delivery to
  whichever consumer picks the partition up next. The existing downstream
  dedupe (§2) already has to tolerate redelivery for other reasons, so this
  doesn't break correctness, but it's a known gap, not something this
  design solves.
- **Poison messages on the Kafka side**: a message that fails to
  deserialize will never reach the "publish to Service Bus" step, so the
  offset-after-publish rule above would otherwise replay it forever and
  stall every message behind it on that partition. Deserialization
  failures do not go through the commit-after-publish path — catch them
  explicitly, publish the raw payload plus the error to a
  `inventory-events-dlq` Kafka topic (or, if none exists yet, log at
  `Critical` with the raw payload attached and alert), **then** report the
  offset to the same `PartitionOffsetCommitTracker` as a normal completion
  — not a direct `consumer.Commit` — so it folds correctly into whatever
  the partition's low-water mark already is instead of committing ahead of
  still in-flight messages. This is the Kafka-side equivalent of §2's
  `DeadLetterMessageAsync` — a message this consumer cannot parse is not a
  transient failure that redelivery will fix.

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
- **Dynamic validation**: right after the inbound envelope is deserialized,
  `ServiceBusConsumerHostedService.HandleMessageAsync` calls the same shared
  `IIS.WMS.Common.DynamicValidation.IDynamicEventValidator` described in §1,
  using the fixed transport folder `ServiceBus` together with the consumer's
  own `ServiceBusConsumerOptions.QueueName` as the identifier — one template
  per queue, not per event type, since this consumer has no per-message
  "schema handler" concept the way the Kafka side does (blob path e.g.
  `ServiceBus/inventory-state-changed.cs`). The message's
  `ApplicationProperties` (`IReadOnlyDictionary<string, object>`) are adapted
  to the transport-neutral `HeaderLookup` via a local `ToHeaderLookup`
  helper before the call. This runs in its own `try`/`catch`, separate from
  the dispatch `try`/`catch` below, because the two failure modes map to
  different outcomes: a **throw** dead-letters the message
  (`DeadLetterMessageAsync` with reason `"DynamicValidationFailed"`) rather
  than being abandoned for redelivery, since a template exception is not a
  transient infrastructure fault; returning **`false`** completes the
  message without dispatching (logged at `Information`, same "valid but
  deliberately not relayed" semantics as §1); returning **`true`** dispatches
  normally, unaffected by this stage.
- **Concurrency conflict retry (the re-read-and-reapply loop)**: this is
  the one piece of logic every cross-reference to "the concurrency retry
  pattern" in this doc set points to — it lives here, not in Polly (§3),
  because it needs to re-fetch fresh state between attempts, which a blind
  retry of the same delegate cannot do:

```csharp
const int maxAttempts = 3;

for (var attempt = 1; attempt <= maxAttempts; attempt++)
{
    var current = await repository.GetAsync(id, category, cancellationToken);

    try
    {
        await repository.PatchAsync(id, category, current!.ETag!, operations, cancellationToken);
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
- **Kafka inbound**: if the message carries a `Correlation-Id` header
  (`WellKnownHeaderNames.CorrelationId` — matching the header name the
  upstream Nexus/WMS producers already use, not this repo's own invention),
  use it; otherwise generate a new GUID at the point of consumption. The
  same `WellKnownHeaderNames` class (`IIS.WMS.Common.Messaging`, shared
  across transports the same way `HeaderLookup` is — see §1) also names the
  `Deduplication-Id`, `App-Id`, and
  `Type` headers read for the dedup check and structured logging in §1. If
  `App-Id` is missing or empty, `ConsumerHostedService` falls back to this
  service's own identity (`ApplicationOptions.ApplicationId`, bound from the
  `Application` configuration section — `ApplicationOptions` also carries
  `ApplicationName`, enriched onto every Serilog log line in `Program.cs`)
  so a relayed event never carries a blank `AppId` downstream.
- **Kafka → Service Bus**: set the correlation ID as a Service Bus message
  `ApplicationProperties["CorrelationId"]` when relaying (alongside the
  `SessionId` set in §1) — this is the transport-hop property a
  broker-level filter or log line can read without deserializing the body.
  In addition, the full `ICorrelationContext` this consumer built
  (`CorrelationId`, `AppId`, `Types`) travels in the message **body**,
  wrapping the schema's own JSON as `ServiceBusRelayEnvelope.Payload` — see
  `ServiceBusRelayEnvelope` (`Infrastructure/Messaging`). The schema's wire
  contract itself (e.g. `InboundInventoryEventMessage`) still carries no
  correlation id of its own; the envelope is what carries it, not a field
  on the payload type.
- **Service Bus consumer**: deserialize the body as `ServiceBusRelayEnvelope`
  first, then its `Payload` as the schema's own type. Read the correlation
  id from `ApplicationProperties["CorrelationId"]` when present (falling
  back to the envelope's own `CorrelationId`, then a fresh GUID only if
  neither is set) and set the full context — correlation id, `AppId`, and
  `Types` — on `ICorrelationContext` and the Serilog `LogContext` before
  processing, so every log line for this message — including the eventual
  Cosmos DB write and any Blob upload — carries the same ID an operator can
  grep across all three systems.
- Never regenerate the correlation ID mid-flow once one exists upstream —
  regenerating breaks the one thing this mechanism exists to provide: a
  single ID to trace one event end-to-end.

## 5. Blob Storage

Five containers now, grouped by tier — don't mix them. **Hot and cold tiers
are backed by separate Storage accounts**, each with its own connection
info (`BlobStorageOptions.Hot`/`BlobStorageOptions.Cold`, each a
`BlobStorageAccountOptions` with its own `AccountUri` — local dev instead
reads `BlobStorage:Hot:ConnectionString`/`BlobStorage:Cold:ConnectionString`
directly off configuration, same pattern as the single-account case used to).
`BlobStorageServiceCollectionExtensions.AddBlobStorage` registers a keyed
`BlobServiceClient`/`IFileStore` pair per tier
(`BlobStorageServiceCollectionExtensions.HotTierKey`/`ColdTierKey`) rather
than one shared instance; `ConsumerHostedService` resolves both via
`[FromKeyedServices]` and picks the tier that matches each write below —
never assume the two containers share an account or a client.

- **Hot tier** (`imports`, `exports` containers): import/export files that
  are actively read/written as part of normal operation (e.g., a bulk
  inventory import a warehouse uploads, or an export a downstream consumer
  polls for). Standard hot-tier blob access, `BlobClient`/`BlobContainerClient`,
  behind an interface (`IFileStore`) so callers don't depend on the Azure
  SDK directly.
- **Hot tier, consumer dead-letter** (`consumer-dead-letter` container,
  `BlobStorageOptions.ConsumerDeadLetterContainerName`): written whenever a
  Kafka message's processing fails past the point of no automatic recovery
  (§1) — kept in its own container rather than folded into
  `imports`/`exports`, since it's a failure record, not an import/export
  file. Content shape depends on which stage failed: **raw bytes**
  (`.bin`) if deserialization (or the immediate JSON-serialize step after
  it) failed - there's no successfully deserialized value to serialize;
  otherwise (validation or Service Bus publish failed) the deserialized
  value as **JSON** (`.json`), since a valid value exists by then. A
  separate, out-of-band watcher process reprocesses whatever lands here.
- **Cold tier, general request/response audit** (`request-audit`
  container, feature-flagged via `BlobStorageOptions.RequestAuditEnabled`):
  **optional**, enabled per environment via configuration, not hardcoded
  on. When enabled, write with `AccessTier.Cold` and name blobs
  `{yyyy}/{MM}/{dd}/{correlationId}.json`.
- **Cold tier, Kafka consumer audit** (also the `request-audit` container,
  `BlobStorageOptions.RequestAuditContainerName`): every message the Kafka
  consumer successfully deserializes gets its JSON representation written
  as an audit blob (§1) — **unconditional**, not gated by
  `RequestAuditEnabled` above; that flag covers the general request/response
  audit use only. Named
  `{correlationId}/{ConsumerHostedServiceName}/{SchemaName}/{timestamp}_{guid}.json`
  — a different convention from the general audit blobs immediately above,
  chosen so one correlation id's full audit trail (across however many
  consumers/schemas touch it) sits under one prefix. Both naming schemes
  share the same container; don't assume every blob under `request-audit`
  follows the `{yyyy}/{MM}/{dd}` shape. **Known trade-off**: because this is
  the *deserialized* value's JSON, not the raw wire bytes, any field a
  newer producer's schema added that this consumer's reader schema doesn't
  recognize is silently absent here - it never survives deserialization to
  be included. A message that fails to deserialize at all only ever gets a
  hot-tier raw-bytes record, never a cold-tier one.
- **Cold tier, Cosmos-mutation audit archive** (`audit-archive` container,
  `BlobStorageOptions.AuditArchiveContainerName`, **plus** a second copy in
  the `request-audit` container, `BlobStorageOptions.RequestAuditContainerName`):
  every Cosmos mutation made through `CosmosRepository{TDomain,TDocument}` is
  captured as an `AuditEntry` and enqueued onto a bounded channel
  (`Persistence.CosmosDb.Audit.AuditTrailWriter`); `AuditBackgroundService`
  drains that channel and fans each entry out to every registered
  `IAuditSink`. Which sink(s) run is configured independently via
  `Audit:CosmosDbEnabled` (persist to the Cosmos `AuditLog` container,
  default `true`) and `Audit:ColdStorageEnabled` (also archive to Blob
  Storage, default `false`) — when both are `true`, every entry is persisted
  to both destinations, and at least one must be `true` or `AddAuditTrail`
  throws at startup. When `ColdStorageEnabled` is `true`, `ColdBlobAuditSink`
  writes **two** blobs per entry, independently of each other (a failure
  writing one never blocks the other — each upload has its own
  `try`/`catch`, logged and swallowed on its own):
  - `audit-archive`, named
    `{ContainerName}/{TimestampUtc:yyyyMMddHHmmssffffff}_{CorrelationId}_{Schema}_{EntityId}__{EntityPartitionKey}_{Operation}_{Guid}.json`
    — a different convention from the general request/response audit's
    date-partitioned `{yyyy}/{MM}/{dd}/...` shape above, chosen so every
    field needed to identify an entry is visible directly in the blob name
    rather than requiring a download.
  - `request-audit` (the same container the Kafka consumer's own audit
    blobs above live in), named
    `{CorrelationId}/Entity/{ContainerName}/{same file name as the audit-archive blob above}.json`
    — the fixed `Entity` folder segment is what keeps this copy from
    colliding with the Kafka consumer's own
    `{correlationId}/{ConsumerHostedServiceName}/{SchemaName}/...` blobs
    already in this container; reusing the identical file name (including
    the same `Guid`) as the `audit-archive` copy lets both copies of one
    logical write be cross-referenced by name alone. This container now
    holds three distinct folder shapes — the general request/response audit's
    `{yyyy}/{MM}/{dd}/...`, the Kafka consumer's
    `{correlationId}/{ConsumerHostedServiceName}/{SchemaName}/...`, and this
    `{CorrelationId}/Entity/{ContainerName}/...` one — don't assume every
    blob under `request-audit` follows any single one of them.

  `EntityPartitionKey` can be a composite key containing `:`
  (cosmos-db.instructions.md §4) — replaced with `-` in the blob name since a
  raw colon breaks filesystem-mapping tools (blobfuse, Storage Explorer
  download) even though Blob Storage itself permits it. The trailing `Guid`
  is a fresh `Guid.NewGuid()` generated at write time, independent of
  `AuditEntry.Id`. Both writes are non-fatal — logged and swallowed, same
  "diagnostic aid, not the durability boundary" treatment as the rest of
  this section — and independent of the Cosmos sink's own hot-tier
  `audit-dead-letter` fallback (`BlobStorageOptions.AuditDeadLetterContainerName`),
  which only fires when the Cosmos write itself fails, not as a cold-storage
  archival path. `Audit:ExcludedContainers` (string array, empty by default)
  names containers to skip audit capture for entirely -
  `AuditTrailWriter.Enqueue` drops a matching entry (matched case-insensitively
  against `AuditEntry.ContainerName`) before it ever reaches the channel, so
  neither sink ever sees it.
- All five go through Polly (§3) for transient fault handling — the Kafka
  consumer's own audit writes (`ConsumerHostedService.WriteBlobAsync`)
  additionally treat a Blob Storage outage (after Polly's retries are
  exhausted) as non-fatal: logged and swallowed, so a Blob Storage outage
  doesn't block the dedup check or the relay itself. The audit trail is a
  diagnostic aid, not the durability boundary (Service Bus is, per §1 -
  though see §1's note on Service Bus publish failures also being handled
  as non-fatal now).
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
        await repository.PatchAsync(item.Id, item.Category, item.ETag, item.Operations, cancellationToken);
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

### `Channel<T>` vs `SemaphoreSlim`/`Parallel.ForEachAsync`

These solve different problems — reach for the right one instead of
treating them as interchangeable concurrency knobs:

- **`SemaphoreSlim`/`Parallel.ForEachAsync`** bound fan-out *within* one
  already-dispatched unit of work — e.g. one Service Bus message expands
  into many Cosmos writes (the example earlier in this section). The
  caller has a known, small batch in hand and just needs to cap how much
  of it runs at once.
- **`System.Threading.Channels.Channel<T>`** decouples the *rate something
  arrives* from the *rate something is processed*, with backpressure — a
  bounded channel blocks the producer once full instead of buffering an
  unbounded backlog in memory. Reach for it when a fast, cheap producer
  (a Kafka poll loop) is paired with a slower, I/O-bound consumer (a
  publish/write call), and you want several concurrent workers draining
  one shared queue rather than processing strictly one item at a time.
  `ConsumerHostedService` (§1) is this repo's worked example:
  bounded `Channel` between the poll loop and `WorkerCount` workers.

**Default to horizontal scaling first.** More Kafka partitions and more
KEDA-scaled pods is still the primary throughput lever — it's simpler to
reason about and doesn't disturb per-partition offset ordering. Reach for
an in-process `Channel` only when profiling shows the bottleneck is
per-pod concurrency itself (the poll loop is starved waiting on a slow
downstream call), not partition/pod count.

```csharp
var channel = Channel.CreateBounded<TItem>(new BoundedChannelOptions(capacity)
{
    FullMode = BoundedChannelFullMode.Wait, // backpressure, not an unbounded buffer
    SingleWriter = true,                    // one producer (the poll loop)
    SingleReader = false,                   // many concurrent workers
});

var workers = Enumerable.Range(0, workerCount)
    .Select(_ => Task.Run(async () =>
    {
        await foreach (var item in channel.Reader.ReadAllAsync(CancellationToken.None))
        {
            await ProcessAsync(item); // the slow, I/O-bound step
        }
    }))
    .ToArray();

// producer: await channel.Writer.WriteAsync(item, stoppingToken) per item, then
// channel.Writer.Complete() and await Task.WhenAll(workers) on shutdown.
```

**The pitfall this pattern introduces: out-of-order completion.** The
moment more than one worker drains the same channel, items can finish in a
different order than they arrived. Any commit/ack/sequence-advance tied to
arrival order (a Kafka offset, a checkpoint, a "last processed" watermark)
must NOT be issued by whichever worker happens to finish first — that can
commit past an earlier item that's still in flight, and on a crash that
item is never redelivered. Track a low-water mark per ordering key
(per Kafka partition, per shard, whatever the ordering scope is) and only
advance/commit it once every earlier item has completed, folding in
early-arriving completions once the gap closes — see
`PartitionOffsetCommitTracker` (§1) for the reference implementation.
Sizing: pick `WorkerCount` from the target throughput and the downstream
call's own latency (`workers ≈ throughput × avg_latency_seconds`) as a
starting point, then load-test — the real ceiling is often the downstream
service's own throughput unit (a Service Bus namespace, a Cosmos RU
budget), not application-level concurrency, and `ChannelCapacity` should
be large enough to absorb a burst without either blocking the poll loop
constantly or growing unbounded.

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

- A `ConsumerHealthCheck` instance per event type a Kafka consumer registers
  via `RegisterSchemaHandlers` (e.g. `InventoryStateChangedConsumerHostedService`
  gets two, one for `InventoryStateChanged` and one for `InventoryAdjusted`,
  since it relays both off one topic/consumer group), each bound to that
  event type's own `ConsumerHealthState` — built internally by the consumer,
  not injected through its constructor, since the set of event types isn't
  known until `RegisterSchemaHandlers` runs. On the **Kafka consumer Pod's**
  `/health/ready`: unhealthy if that event type's last successful poll
  exceeds a configured staleness window (it doesn't need a message to
  process to be healthy — an idle topic isn't a failure, so every event
  type's state advances on every poll cycle, not just whichever type a
  given poll happened to return).
- `ServiceBusHealthCheck`, on the **Service Bus consumer Pod's**
  `/health/ready`: verifies the management client can reach the namespace.

## 9. Testing this layer

- **Unit tests** (xUnit): mock `IConsumer<,>`, `ServiceBusSender`/
  `ServiceBusProcessor`, `IFileStore`, and `IDeduplicationService`; test
  message mapping, the dedup composite-key construction and 409→duplicate
  parsing in `NexusDeduplicationService`, and the retry/dead-letter decision
  tree in isolation. Follow
  `MethodName_Condition_ExpectedResult()` per
  [dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md) §4.
- **Integration tests**: exercise the full Kafka → Service Bus → Cosmos DB
  path for at least: happy path, duplicate/redelivered message, and a forced
  Cosmos `412 PreconditionFailed` to prove the concurrency retry loop in §2
  above actually re-reads and re-applies against the fresh ETag (the
  exception itself is raised the way
  [cosmos-db.instructions.md](cosmos-db.instructions.md) §9 shows). No
  Cosmos DB Emulator/Azure Service Bus emulator container image was ever
  successfully wired up for this (`KafkaRelayContainerTests` covered only
  the Kafka leg on its own) — the standard is now:
  - **Kafka**: `Testcontainers` spinning up a real (containerized) broker —
    unchanged, still not a mock.
  - **Service Bus**: a **Virtual Service Bus** — an in-process fake built by
    subclassing the Azure SDK's own mockable client types
    (`ServiceBusClient`/`ServiceBusSender` on the publish side;
    `ServiceBusSessionProcessor`/`ServiceBusProcessor` on the consume side),
    which Microsoft's own SDK design already supports (public/protected
    parameterless constructors, virtual factory methods — see
    `VirtualServiceBusClient`/`VirtualServiceBusSender` in the integration
    test project). This is not a generic mock of the messaging contract —
    the consume side calls the exact same `HandleMessageAsync` core method
    (see below) the real `ServiceBusSessionProcessor`/`ServiceBusProcessor`
    event handler calls in production, so the business logic under test is
    identical either way.
  - **Cosmos DB**: an **in-memory `Container` fake** (`InMemoryCosmosContainer`,
    registered as `ICosmosContainerFactory` in place of the real
    `CosmosContainerFactory`) with faithful `CosmosException`/`HttpStatusCode`
    semantics — real ETag-conflict (`412`), duplicate-create (`409`), and
    missing-item (`404`) behavior, not just enough surface to compile.
  - Subscribing to `ServiceBusSessionProcessor.ProcessMessageAsync`/
    `ServiceBusProcessor.ProcessMessageAsync` on a processor not built
    through a real connected `ServiceBusClient` throws on the subscription
    itself — a hard SDK limitation, confirmed by direct reproduction, not a
    design choice. This is why `ServiceBusConsumerHostedService`/
    `BulkImportServiceBusConsumerHostedService` build their processor
    lazily in `ExecuteAsync` rather than their constructor, and split their
    event handler into a thin SDK-facing adapter plus an `internal
    HandleMessageAsync(ServiceBusReceivedMessage, CancellationToken)` core
    that returns a `ServiceBusMessageOutcome` instead of calling
    `CompleteMessageAsync`/`AbandonMessageAsync`/`DeadLetterMessageAsync`
    directly — an integration test constructs the hosted service normally
    (safe, since the processor isn't built until `ExecuteAsync` runs, which
    the test never calls) and wires `HandleMessageAsync` directly to the
    Virtual Service Bus broker instead.
  - `CosmosRepository`'s two query methods (`GetPagedAsync`/`QueryAsync`)
    call the real SDK's `IQueryable.ToFeedIterator()` extension, which
    requires a genuinely Cosmos-backed queryable and cannot be satisfied by
    any in-memory fake — `ReadNextPageAsync` is a `protected virtual` seam
    on `CosmosRepository` solely for this; a test-only repository subclass
    overrides it to materialize the in-memory queryable directly instead.
- Coverage target and the shared `WebApplicationFactory<Program>` test host
  setup are defined in
  [engineering-standards.instructions.md](engineering-standards.instructions.md) §7 —
  this file doesn't restate the number.
