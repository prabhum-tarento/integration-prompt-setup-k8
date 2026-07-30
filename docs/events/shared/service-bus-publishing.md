# Shared helper — outbound Service Bus publishing

> **Canonical source:** [integration-resiliency.instructions.md](../../ai/integration-resiliency.instructions.md)
> §1 (sender lifecycle, message ID, SessionId) and §3 (Polly
> `service-bus-publish` pipeline). This win over any event doc on conflict.

## Purpose

Several events publish downstream messages (to Nexus Producer, OMS order
tracking, reflex queues, etc.). This helper describes the **one** outbound
publish mechanism they all share, replacing the older `IServiceBusQueueService`
abstraction and the various `TODO` / commented-out sends in the event docs.

> **Architecture-alignment note.** The reference doc
> `inventory.InventoryStateChanged.md` still describes outbound publishing via
> `IServiceBusQueueService`. That abstraction is **not** defined in the
> instruction docs; the canonical mechanism is a cached `ServiceBusSender`
> (below). Regenerated event docs follow the canonical mechanism.

## Mechanism — cached `ServiceBusSender`

- One `ServiceBusSender` is **cached per distinct queue name** (keyed off the
  resolved queue name, not per schema), reused for the app's lifetime per
  Microsoft's Service Bus client-lifetime guidance — **never opened per
  message**. The owning hosted service is a singleton and implements
  `IAsyncDisposable`, so senders are disposed on graceful shutdown (SIGTERM
  within `terminationGracePeriodSeconds`); a hard SIGKILL/OOM relies on the
  broker's idle-connection reclaim.
- Business/application code never touches `ServiceBusSender` directly — it goes
  through the application-layer publish abstraction, which owns the cache.

## Queue names (kebab-case, config-resolved)

Queue names are **kebab-case** and resolved from the `ServiceBus` configuration
section — never hard-coded string literals or env-var constants at the call
site. The old env-var name is retained only as a traceability note:

| Config key / queue name | Old constant | Typical producers |
|---|---|---|
| `nexus-producer` | `NEXUS_PRODUCER_QUEUE_NAME` | inventory + B2B events → SAP/Nexus |
| `nexus-b2cstock-producer` | `NEXUS_B2CSTOCK_PRODUCER_QUEUE_NAME` | StockSyncSubmitted B2C stock |
| `order-tracking` | `ORDER_TRACKING_QUEUE_NAME` | OMS order-tracking updates |
| `inventory-adjusted-reflex` | `INVENTORY_ADJUSTED_REFLEX_QUEUE_NAME` | hallmarking → inventory-adjusted reflex |

## Message shape and identity

- **Payload** is wrapped in a `ServiceBusRelayEnvelope` (`Payload` /
  `ReflexSchema`), the same envelope used across the relay.
- **Message ID must be deterministic across redelivery** — derive it from the
  Kafka key or a stable payload field (`WarehouseId:Sku:EventId`), never a fresh
  GUID. This is what makes the downstream consumer's dedupe check work.
- **`SessionId` = `{WarehouseId}:{Sku}`** — same component order as the Cosmos
  partition key, grouping all messages for one aggregate into one ordered
  session.
- **Correlation** is propagated via `ICorrelationContext` and the
  `WellKnownHeaderNames` headers (correlation id, dedup id, type) copied onto
  the outbound message.

## Resilience — Polly v8 `service-bus-publish` pipeline

Every publish is wrapped in the keyed `service-bus-publish` resilience
pipeline (retry on transient `ServiceBusException`, exponential backoff with
jitter, `MaxRetryAttempts = 5`). This pipeline handles **only** transient
Service Bus faults — it does **not** wrap Cosmos `429`/`412` (those are the SDK
retry and the §2 re-read loop respectively). Do not add Cosmos exceptions to
its `ShouldHandle`.

## Processing rules

- **Publish only after the state change is durably applied.** Outbound
  notifications reflect committed state; publishing before the Cosmos write
  succeeds risks announcing a change that later fails.
- **Idempotent downstream.** Because the message ID is deterministic and
  sessions preserve order, a redelivered outbound message is de-duplicated by
  the consumer rather than double-applied.
- **Fan-out to multiple queues** (e.g. `nexus-producer` + `order-tracking`) is
  independent publishes, each through its own cached sender and the shared
  pipeline; a failure on one is retried on its own without re-sending the
  other.

## Edge cases

- **Oversized payload:** claim-check offload to the hot-tier large-payload blob
  container; the envelope carries `BlobPath` and the consumer rehydrates it
  (integration §1).
- **Publish exhausts retries:** surfaces as a processing failure for the
  triggering message → outcome mapping in
  [cosmos-idempotent-write.md](cosmos-idempotent-write.md).
- **No queue configured for a key:** fail fast at startup/registration rather
  than silently dropping the message.
