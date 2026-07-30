# inventory.InventoryAdjusted - Technical Documentation

## 1. Overview

### Purpose
`inventory.InventoryAdjusted` is a Kafka event that processes inventory
adjustment events from the WMS. It updates inventory across fulfilment
locations, applies B2B/B2C segmentation, tracks non-standard (extended) states,
and propagates deltas to SAP (via Nexus), OMS, and ICR.

### Business Objective
- Maintain accurate inventory across warehouses and 3PL facilities.
- Allocate inventory between B2B and B2C channels.
- Segment inventory by hallmarking, country of origin, and fulfilment location.
- Propagate inventory changes to SAP/Nexus, OMS, and ICR.
- Calculate and report signed inventory deltas to dependent systems.

### Scope
- Consumes `inventory.InventoryAdjusted` from Kafka, relays to Azure Service Bus,
  and processes it on a session-enabled Service Bus consumer.
- Performs B2B/B2C segmentation, extended-state transitions, and delta
  calculations.
- Persists inventory to Cosmos DB via ETag-guarded **Patch** operations.
- Publishes downstream events to the `nexus-producer` queue.

### High-Level Architecture

Matches the platform data flow in
[integration-resiliency.instructions.md](../ai/integration-resiliency.instructions.md):
a Kafka-to-Service-Bus relay hosted service, then a session-enabled Service Bus
consumer that calls the Application layer, which persists through the Cosmos DB
repository and archives through Blob Storage.

```
Kafka topic `inventory-events` (Type header: InventoryAdjusted, Avro)
                    ↓
   InventoryAdjustedConsumerHostedService (KafkaConsumerHostedServiceBase)
     - correlation id / dedup id / type headers read + logged
     - Nexus dedup check (IDeduplicationService, fail-open)
     - schema + dynamic validation
     - cold-tier request audit (unconditional)
                    ↓
   Azure Service Bus queue `inventory-adjusted`
   (session-enabled: SessionId = {FulfilmentId}:{ItemCode};
    message ID deterministic from the Kafka key — never a fresh GUID)
                    ↓
   InventoryAdjustedServiceBusHostedService (ServiceBusConsumerHostedService<InventoryAdjustedEvent>)
     - envelope + payload deserialize, dynamic validation, cold-tier audit
                    ↓
          IInventoryAdjustedHandler.HandleAsync
                    ↓
    ┌───────────────┬───────────────┬──────────────┬──────────────┐
    ↓               ↓               ↓              ↓              ↓
B2B Adjusted   Segmentation    Extended-State   OMS Delta      ICR Snapshot
(SAP/Nexus)    (B2B/B2C)       transitions      (OMS)          (reporting)
    ↓               ↓               ↓              ↓              ↓
IItemStockInventoryService → Cosmos DB (ETag-guarded Patch, re-read-and-reapply
on 412) + MessageArchive (Cosmos, optional Blob cold-tier mirror)
                    ↓
        nexus-producer queue (Service Bus) via cached ServiceBusSender
```

Business logic never touches `CosmosClient`/`Container`/`ServiceBusSender`
directly — it goes through `IItemStockInventoryService` → the Cosmos repository
and through the application-layer publish abstraction (see
[shared/service-bus-publishing.md](shared/service-bus-publishing.md)).

### Key Dependencies
- **`ItemStockInventoryRepository`** — core inventory (Cosmos, multi-container
  EDC/TDC/ADC/CAECOM/BRZ3PL, ETag-guarded; cosmos §5a/§9).
- **`ItemLevelSegmentationRepository`** / **`FulfilmentLevelSegmentationRepository`**
  — segmentation rules (Cosmos, read-only).
- **`ItemStockInventoryExtendedRepository`** — non-standard state tracking (Cosmos).
- **`CountryRepository`** — country/market mapping (Cosmos, read-only).
- **`MessageArchiveRepository`** — snapshot archival (Cosmos + optional Blob).
- **Cached `ServiceBusSender`** — outbound Nexus publishing.
- Shared helpers: [segment-inventory](shared/segment-inventory.md),
  [b2c-extension-calculation](shared/b2c-extension-calculation.md),
  [inventory-formulas](shared/inventory-formulas.md),
  [delta-towards-oms](shared/delta-towards-oms.md),
  [icr-snapshot](shared/icr-snapshot.md),
  [country-code-lookup](shared/country-code-lookup.md),
  [archive-audit](shared/archive-audit.md),
  [cosmos-idempotent-write](shared/cosmos-idempotent-write.md),
  [service-bus-publishing](shared/service-bus-publishing.md).

### Assumptions
1. Incoming messages are valid `inventory.InventoryAdjusted` Avro objects
   deserialized to `InventoryAdjustedEvent`.
2. Fulfilment location IDs map to known centers (TDC, EDC, ADC, CAECOM, BRZ3PL).
3. Negative quantities represent deductions; normalized per
   [inventory-formulas](shared/inventory-formulas.md).
4. Country codes resolve from `CountryRepository` or fall back to `UNKNOWN`.
5. **Processing is idempotent** — a deterministic document `Id` plus ETag-guarded
   Patch make redelivery a no-op, not a duplicate/double-count (see
   [cosmos-idempotent-write](shared/cosmos-idempotent-write.md)).

---

## 2. End-to-End Flow

```
1. MESSAGE RECEPTION (Kafka consumer)
   ├─ InventoryAdjusted deserialized (Avro → InventoryAdjustedEvent)
   ├─ correlation/dedup/type headers logged; IDeduplicationService check (fail-open)
   ├─ schema + dynamic validation; cold-tier request audit
   └─ relay to Service Bus queue `inventory-adjusted`
        · SessionId = {FulfilmentId}:{ItemCode}
        · deterministic message ID from Kafka key (never a fresh GUID)

2. SERVICE BUS CONSUMPTION
   ├─ envelope + payload deserialize, dynamic validation, cold-tier audit
   └─ IInventoryAdjustedHandler.HandleAsync(InventoryAdjustedEvent)

3. ADJUSTMENT LINE ITERATION
   For each AdjustmentLine:
   ├─ build SegmentationInputModel (ProductId, CountryOfOrigin, Hallmark,
   │  Quantity, LocationType) + deterministic uniqueIdentifier (ItemCode, LineNo, ReferenceId)

   4. B2B ADJUSTED/MOVED (SAP) — see delta-towards-oms.md
      trigger: ENABLE_DELTA_TOWARDS_SAP AND (Location != ADC OR ENABLE_ADC_DELTA_TOWARDS_AX12)
      ├─ ToState = (UNKNOWN,UNKNOWN) if Quantity < 0 else Adjustment.State
      ├─ SAE-2798 / SAE-3032 fixes; quantity normalization (Math.Abs)
      └─ publish Inventory_B2BInventoryAdjustedOrMoved → nexus-producer

   5. SEGMENTATION (AVAILABLE + PICKABLE) — see segment-inventory.md
      ├─ fetch/create ItemStockInventory
      ├─ inboundQty = signed normalization; validate (no negate-empty)
      ├─ 3PL → fulfilment-level; WH → item-level (if active, IsExtended) else fulfilment-level
      ├─ delta = currB2CAVL - prevB2CAVL; IsB2CChanged
      ├─ archive before/after (archive-audit.md)
      └─ PERSIST via ETag-guarded Patch (Increment/Set), 412 re-read/reapply loop

   5b. EXTENDED-STATE TRANSITIONS (other states)
      ├─ TO-state: create-if-missing then Patch Increment
      └─ FROM-state: validate sufficient qty, Patch Increment (negative)

   6. OMS DELTA (ENABLE_DELTA_TOWARDS_OMS[/_3PL] AND IsB2CChanged) — delta-towards-oms.md
      └─ publish Inventory_B2CInventoryAdjusted → nexus-producer

   7. ICR SNAPSHOT (ENABLE_SNAPSHOT_FOR_ICR) — icr-snapshot.md
      └─ publish Inventory_OmniInventoryAvailabilityReported → nexus-producer

8. OUTCOME
   └─ no exception → Completed; ConcurrencyException/OperationCanceled → Abandoned;
      any other → DeadLettered (see cosmos-idempotent-write.md)
```

### Data Flow Through Layers
`Kafka → KafkaConsumerHostedServiceBase → Service Bus (inventory-adjusted) →
ServiceBusConsumerHostedService → IInventoryAdjustedHandler → helpers →
IItemStockInventoryService → Cosmos repository (Patch/ETag) + archive →
ServiceBusSender (nexus-producer)`.

---

## 3. Detailed Business Logic

### 3.1 B2B Adjusted/Moved (SAP/Nexus)
See [delta-towards-oms.md](shared/delta-towards-oms.md) for the full builder,
trigger conditions, and the SAE-2798/SAE-3032 fixes. Event-specific inputs:
- **ToState by quantity sign:** `Quantity < 0` → `(UNKNOWN, UNKNOWN)`;
  otherwise `Adjustment.State`.
- Publishes `Inventory_B2BInventoryAdjustedOrMoved` to `nexus-producer`.

### 3.2 B2C Segmentation
See [segment-inventory.md](shared/segment-inventory.md). Trigger: `State ==
AVAILABLE AND Status == PICKABLE`. 3PL → fulfilment-level; warehouse →
item-level extension when an active rule exists (sets `IsExtended`), else
fulfilment-level. Delta = current − previous B2C available; `IsB2CChanged` =
(delta ≠ 0).

### 3.3 Extended-State Transitions
Non-standard states (RESERVED, DEFECTIVE, IN_TRANSIT, etc.) tracked in
`ItemStockInventoryExtended`:
- **TO-state:** create-if-missing (deterministic Id, 409-as-applied) then Patch
  `Increment(+qty)`.
- **FROM-state:** only when existing `Qty ≥ |inboundQty|`; Patch
  `Increment(-qty)`. Insufficient → warning, skip (no negative extended qty).

### 3.4 OMS Delta & 3.5 ICR Snapshot
Delegated wholesale to [delta-towards-oms.md](shared/delta-towards-oms.md) and
[icr-snapshot.md](shared/icr-snapshot.md).

---

## 4. Calculation Logic

All quantity math is centralized in
[inventory-formulas.md](shared/inventory-formulas.md) and
[b2c-extension-calculation.md](shared/b2c-extension-calculation.md).

- **Inbound quantity:** `inboundQty = Convert.ToInt32(MoveSign + Quantity)`
  (signed). Examples: `("",100)→100`, `("-",75)→-75`, `("+",50)→50`.
- **Delta towards OMS:** `currB2CAVL − prevB2CAVL`.

| Previous | Current | Delta | IsB2CChanged |
|---|---|---|---|
| 100 | 150 | +50 | true |
| 100 | 75 | −25 | true |
| 100 | 100 | 0 | false |

Increments are applied with `PatchOperation.Increment`, never
read-modify-write-replace.

---

## 5. Database Documentation

All Cosmos access follows [cosmos-db.instructions.md](../ai/cosmos-db.instructions.md)
and [cosmos-idempotent-write.md](shared/cosmos-idempotent-write.md).

### 5.1 ItemStockInventory (Cosmos, multi-container per fulfilment code)
- **Partition key** `Category` = composite `FulfilmentId:ItemCode:Hallmark:CountryOfOrigin`.
- **Read:** `GetAsync(id, category)` — point read within one partition.
- **Create (first write):** deterministic `Id`; `409 Conflict` → return existing
  (redelivery no-op).
- **Update:** **`PatchAsync`** with `IfMatchEtag`, `PatchOperation.Increment` for
  B2B/B2C quantities and `.Set` for flags (`IsExtended`) and `ModifiedUtc`,
  **≤10 ops**. `412` → `ConcurrencyException` → §2 re-read/reapply loop (max 3).
- **No last-write-wins** on any quantity field.

| Field | How derived |
|---|---|
| B2BAVL / B2CAVL / B2COrg | segmentation + extension helpers → Patch Increment |
| IsExtended | item-level rule active → Patch Set |
| ModifiedUtc | caller-supplied UTC → Patch Set |

### 5.2 ItemLevelSegmentation / FulfilmentLevelSegmentation (Cosmos, read-only)
Point reads by category; supply `EcomShare`, `IsActive`, `StoreLeveragePercentage`.

### 5.3 ItemStockInventoryExtended (Cosmos)
Composite key incl. State/Status; Patch `Increment` for TO/FROM transitions.

### 5.4 Archive
Before/after snapshots via [archive-audit.md](shared/archive-audit.md)
(best-effort; failure does not fail the message).

### 5.5 Transaction Flow & Concurrency
Cosmos has no multi-document transactions here; correctness comes from
per-document ETag Patch + the §2 retry loop, not distributed transactions.

---

## 6. State Changes & State Machine

```
AdjustmentLine
   ↓  signed inboundQty (inventory-formulas.md)
Fetch/Create ItemStockInventory (deterministic Id; 409 → existing)
   ↓  archive previous
Apply segmentation (3PL | item-level | fulfilment-level)
   ↓  compute delta, IsB2CChanged
Patch (ETag, Increment/Set)  ── 412 ─▶ re-read + reapply (≤3)
   ↓  archive new
Publish downstream (nexus-producer) after durable commit
   ↓
Final: inventory updated exactly once
```

**Critical invariants:** no quantity goes negative; extended FROM-state never
decremented below zero; a redelivered message produces no additional mutation.

---

## 7. API Documentation

### Kafka message contract
Topic `inventory-events`, `Type` header `InventoryAdjusted`, Avro payload
mapped to `InventoryAdjustedEvent`:

```json
{
  "Channel": "B2B|B2C",
  "Adjustment": {
    "ReferenceId": "ABC123-XYZ789",
    "Location": { "Id": "WAREHOUSE_1", "Type": "WAREHOUSE|THIRD_PARTY_LOGISTICS" },
    "State": { "State": "AVAILABLE|RESERVED|...", "Status": "PICKABLE|PREPARED|..." },
    "AdjustmentLines": [
      { "ProductId": "SKU-001", "LineNum": "1", "Quantity": 100,
        "CountryOfOrigin": "INDIA", "Hallmarking": "916" }
    ]
  }
}
```

### Service Bus message contract
Queue `inventory-adjusted`, `ServiceBusRelayEnvelope` wrapping the event;
`SessionId = {FulfilmentId}:{ItemCode}`; deterministic `MessageId`; correlation
headers per [service-bus-publishing.md](shared/service-bus-publishing.md).

### Validation
| Field | Rule | Handling |
|---|---|---|
| payload | not null / schema-valid | poison → DeadLettered |
| Quantity | integer | signed parse |
| State/Status | valid enum | reject invalid |
| Location.Id | resolvable | fallback `CountryCode.UNKNOWN` |
| negative qty on empty inventory | invalid | business rejection, skip line |

---

## 8. Error Handling & Retry Mechanisms

- **Validation / poison payload** → DeadLettered (hot-tier dead-letter container).
- **Cosmos 412 (ETag)** → `ConcurrencyException` → §2 re-read/reapply loop (≤3);
  if exhausted → Abandoned (redelivered up to `MaxDeliveryCount`).
- **Cosmos 429** → Cosmos SDK retry (`MaxRetryAttemptsOnRateLimitedRequests`).
- **Service Bus publish transient** → `service-bus-publish` Polly pipeline.
- **`OperationCanceledException`** → Abandoned.
- **Any other exception** → DeadLettered (`Reason` = type, `Description` =
  `ex.ToString()`).
- **Application rejections** (`MissingItemStockInventoryException`,
  `InvalidExtendedItemStockInventoryQtyException`) → logged; that line is
  skipped without failing the whole message.

Outcome mapping is the definitive table in
[cosmos-idempotent-write.md](shared/cosmos-idempotent-write.md).

---

## 9. Security & Configuration

### Authentication
- Cosmos DB and Service Bus use **connection strings** sourced from Azure Key
  Vault (delivered as a Kubernetes Secret); local dev uses the emulator /
  user-secrets. This is the deliberate documented standard (cosmos §1) — not
  Managed Identity.

### Feature flags
| Flag | Default | Purpose |
|---|---|---|
| ENABLE_DELTA_TOWARDS_SAP | true | B2B SAP events |
| ENABLE_ADC_DELTA_TOWARDS_AX12 | false | ADC-specific SAP events |
| ENABLE_DELTA_TOWARDS_OMS | true | OMS B2C notifications (warehouse) |
| ENABLE_DELTA_TOWARDS_OMS_3PL | true | OMS B2C notifications (3PL) |
| ENABLE_SNAPSHOT_FOR_ICR | false | ICR snapshots |

### Queue names (kebab-case, config-resolved)
| Queue | Old constant | Direction |
|---|---|---|
| `inventory-adjusted` | INVENTORY_ADJUSTED_REFLEX_QUEUE_NAME | inbound (relay) |
| `nexus-producer` | NEXUS_PRODUCER_QUEUE_NAME | outbound |

### Data protection
TLS in transit; encryption at rest; no secrets/keys logged.

---

## 10. Known Limitations & Future Improvements

### Current Limitations
- Integer quantities only (no fractional units).
- Segmentation rules read per line; may be cached (see below).
- Extended FROM-state with insufficient quantity is skipped with a warning
  rather than reconciled.

### Potential Improvements
- Cache country codes and segmentation rules per process to cut Cosmos reads.
- Batch downstream publishes where a line produces multiple events.
- Evaluate parallel line processing within one message (bounded), preserving
  per-aggregate ordering via the session.

> The previous version listed "TODO: message queuing not implemented" and
> "no idempotency / ETag conflicts on concurrent updates" as gaps. Both are now
> resolved by design: downstream sends go through the cached `ServiceBusSender`
> (§9) and redelivery/concurrency are handled by deterministic Id + ETag Patch +
> the §2 loop.

---

## 11. Summary

`inventory.InventoryAdjusted` processes WMS adjustments on the AKS pipeline:
consumes from Kafka, relays to the `inventory-adjusted` Service Bus queue,
applies B2B/B2C segmentation and extended-state transitions, and publishes
deltas/snapshots to `nexus-producer`.

**Key business logic:** B2B negative → UNKNOWN state; 3PL vs warehouse
segmentation precedence; extended-state increment/decrement guards; OMS delta
only when B2C changed; before/after archival.

**Database updates:** ETag-guarded **Patch** (`Increment`/`Set`, ≤10 ops) on
`ItemStockInventory` and `ItemStockInventoryExtended`, with deterministic Id +
409 handling and the §2 412 re-read/reapply loop — this is the fix for the
duplicate-entry / doubled-quantity problem.

**Risks & recommendations:** concurrency conflicts are expected to be rare once
sessions are in place; monitor dead-letter counts and Cosmos 429 rates; cache
rarely-changing lookups.

---

**Document Version:** 2.0 (AKS / k8s)
**Status:** Regenerated
