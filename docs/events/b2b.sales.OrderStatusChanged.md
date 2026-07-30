# b2b.sales.OrderStatusChanged - Technical Documentation

## 1. Overview

### Purpose
`b2b.sales.OrderStatusChanged` is a Kafka event that processes order status
change events from the Order Management System (OMS). It normalizes warehouse
codes, selects the appropriate order identifier based on fulfilment-centre type,
maps the OMS status to the tracking-system status, and publishes a standardized
order-tracking request downstream.

### Business Objective
- Capture order status transitions (CANCELLED, DELETED, or other statuses) from
  OMS.
- Produce standardized tracking requests routed to the `order-tracking` queue.
- Maintain accurate order-tracking information across fulfilment units (TDC, ADC,
  and standard warehouses) and channels.

### Scope
- Consumes `b2b.sales.OrderStatusChanged` from Kafka (consumer group
  `$OrderStatusChangedIIS`), deserializes to `OrderStatusChangedEvent`, and
  relays to a session-enabled Azure Service Bus queue.
- Performs warehouse classification, reference-ID selection, status mapping, and
  fulfilment-unit-ID normalization.
- **Performs no database operations** — it neither reads nor writes Cosmos DB.
- Publishes an `OrderTrackingCommonRequest` to the `order-tracking` queue via a
  cached `ServiceBusSender`.

### High-Level Architecture

Matches the platform data flow in
[integration-resiliency.instructions.md](../ai/integration-resiliency.instructions.md):
a Kafka-to-Service-Bus relay hosted service, then a session-enabled Service Bus
consumer that calls the Application layer, which builds the tracking request and
publishes it — with no persistence step in between.

```
Kafka topic `b2b.sales.OrderStatusChanged` (Avro → OrderStatusChangedEvent)
                    ↓
   OrderStatusChangedConsumerHostedService (KafkaConsumerHostedServiceBase)
     - correlation id / dedup id / type headers read + logged
     - IDeduplicationService check (fail-open)
     - schema + dynamic validation
     - cold-tier request audit (unconditional)
                    ↓
   Azure Service Bus queue `order-status-changed`
   (session-enabled: SessionId = {WarehouseCode}:{OrderId};
    message ID deterministic from the Kafka key — never a fresh GUID)
                    ↓
   OrderStatusChangedServiceBusHostedService (ServiceBusConsumerHostedService<OrderStatusChangedEvent>)
     - envelope + payload deserialize, dynamic validation, cold-tier audit
                    ↓
          IOrderStatusChangedHandler.HandleAsync
     - warehouse classification → reference-ID selection
     - status mapping → fulfilment-unit-ID normalization
     - build OrderTrackingCommonRequest
                    ↓
        order-tracking queue (Service Bus) via cached ServiceBusSender
```

Business logic never touches `ServiceBusSender` directly — it goes through the
application-layer publish abstraction (see
[shared/service-bus-publishing.md](shared/service-bus-publishing.md)). There is
no Cosmos repository in this flow.

### Key Dependencies
- **Cached `ServiceBusSender`** — outbound `order-tracking` publishing.
- **`IDeduplicationService`** — Kafka-side dedup (fail-open) keyed on the
  `Deduplication-Id` header.
- **`ICorrelationContext`** — correlation propagation onto the outbound message.
- **`ReflexConstants`** — warehouse-code definitions (`TdcSapId` = "D001",
  `TDCFulfilmentId` = "TDC", `ADCFulfilmentId` = "ADC").
- **`OrderStatusChangedEvent`** — input event model.
- **`OrderTrackingCommonRequest`** — output request model (the builder documented
  in [delta-towards-oms.md](shared/delta-towards-oms.md); the historical
  `OrderTrackingCommonOrchestratorRequest` name is retained only for traceability
  — there is **no orchestrator**).
- Shared helpers: [service-bus-publishing](shared/service-bus-publishing.md),
  [delta-towards-oms](shared/delta-towards-oms.md) (order-tracking request
  builder), [archive-audit](shared/archive-audit.md).

### Assumptions
1. Incoming messages are valid `b2b.sales.OrderStatusChanged` Avro objects
   deserialized to `OrderStatusChangedEvent`.
2. Only three warehouse identifiers are special-cased: `TdcSapId` (D001),
   `TDCFulfilmentId` (TDC), `ADCFulfilmentId` (ADC); all others are standard.
3. For TDC/ADC, `PickingRouteId` is the reference id; for standard warehouses,
   `OrderId` is the reference id.
4. **No persistence** — this handler builds and forwards a request only.
5. **Processing is idempotent** — a deterministic outbound message id makes
   redelivery a downstream no-op (dedup at the `order-tracking` consumer), so a
   retried Kafka message does not create a duplicate tracking request.

---

## 2. End-to-End Flow

```
1. MESSAGE RECEPTION (Kafka consumer)
   ├─ OrderStatusChanged deserialized (Avro → OrderStatusChangedEvent)
   ├─ correlation/dedup/type headers logged; IDeduplicationService check (fail-open)
   ├─ schema + dynamic validation; cold-tier request audit
   └─ relay to Service Bus queue `order-status-changed`
        · SessionId = {WarehouseCode}:{OrderId}
        · deterministic message ID from Kafka key (never a fresh GUID)

2. SERVICE BUS CONSUMPTION
   ├─ envelope + payload deserialize, dynamic validation, cold-tier audit
   └─ IOrderStatusChangedHandler.HandleAsync(OrderStatusChangedEvent)

3. WAREHOUSE CLASSIFICATION
   ├─ isNotTDCorADC = warehouseCode ∉ { TdcSapId(D001), TDCFulfilmentId(TDC), ADCFulfilmentId(ADC) }
   └─ (uses OrdinalIgnoreCase set membership; see §4)

4. REFERENCE-ID SELECTION
   └─ orderId = isNotTDCorADC ? OrderId : PickingRouteId

5. STATUS MAPPING
   └─ CANCELLED → OrderTrackingStatus.CANCELLED
      DELETED   → OrderTrackingStatus.DELETED
      other     → OrderTrackingStatus.UNKNOWN

6. FULFILMENT-UNIT-ID NORMALIZATION
   └─ warehouseCode == TdcSapId(D001) ? TDCFulfilmentId(TDC) : warehouseCode

7. BUILD REQUEST (OrderTrackingCommonRequest — delta-towards-oms.md)
   ├─ ReferenceId = orderId, OrderId = orderId
   ├─ Channel, BackOrderId, FulfilmentUnitId, OrderStatus
   ├─ Lines = [ new OrderTrackingLine() ]
   └─ Type = EventType.B2B_ORDER_STATUS_CHANGED

8. PUBLISH
   └─ cached ServiceBusSender → `order-tracking`
      · deterministic MessageId (WarehouseCode:OrderId:Status)
      · correlation headers; service-bus-publish Polly pipeline

9. OUTCOME
   └─ no exception → Completed; OperationCanceled → Abandoned;
      any other → DeadLettered (see cosmos-idempotent-write.md outcome table)
```

### Data Flow Through Layers
`Kafka → KafkaConsumerHostedServiceBase → Service Bus (order-status-changed) →
ServiceBusConsumerHostedService → IOrderStatusChangedHandler → request builder →
ServiceBusSender (order-tracking)`. No Cosmos layer participates.

---

## 3. Detailed Business Logic

### 3.1 Warehouse Classification

**Why it exists:** Different fulfilment units use different id schemes. TDC uses
a SAP id (D001) that must be normalized, and TDC/ADC route on `PickingRouteId`
instead of `OrderId`.

**Rule:**
```
isNotTDCorADC = warehouseCode ∉ { "D001" (TdcSapId),
                                  "TDC"  (TDCFulfilmentId),
                                  "ADC"  (ADCFulfilmentId) }
```

- **Input:** `eventMessage.WarehouseCode`.
- **Output:** `isNotTDCorADC` (boolean), drives reference-id selection.
- **Validation:** a null/empty `WarehouseCode` is a validation failure — the
  message is rejected (DeadLettered) rather than allowed to throw a
  `NullReferenceException`. Comparison uses `StringComparer.OrdinalIgnoreCase`
  (see §4) so casing does not misclassify a warehouse.

| Scenario | WarehouseCode | isNotTDCorADC | Reference id |
|---|---|---|---|
| Standard | "PJCDC" | true | OrderId |
| TDC SAP id | "D001" | false | PickingRouteId |
| TDC fulfilment | "TDC" | false | PickingRouteId |
| ADC fulfilment | "ADC" | false | PickingRouteId |
| Unknown | "UNKNOWN" | true | OrderId (standard default) |

### 3.2 Reference-ID Determination

**Why it exists:** TDC/ADC track on `PickingRouteId`; standard warehouses track
on `OrderId`.

```
orderId = isNotTDCorADC ? eventMessage.OrderId : eventMessage.PickingRouteId
```

- **Output:** `orderId`, used for both `ReferenceId` and `OrderId` on the request.
- **Validation:** if the selected identifier is null/empty for the resolved
  warehouse type, the message is rejected (DeadLettered) — an invalid tracking
  reference is never published downstream.

### 3.3 Status Code Mapping

**Why it exists:** OMS emits `StatusCode`; the tracking system consumes
`OrderTrackingStatus`. Only CANCELLED and DELETED are meaningful here; everything
else maps to UNKNOWN by design.

```
orderStatus = Status switch
{
    StatusCode.CANCELLED => OrderTrackingStatus.CANCELLED,
    StatusCode.DELETED   => OrderTrackingStatus.DELETED,
    _                    => OrderTrackingStatus.UNKNOWN
};
```

| Input StatusCode | Output OrderTrackingStatus |
|---|---|
| CANCELLED | CANCELLED |
| DELETED | DELETED |
| ORDER_CANCELED / CREDIT_BLOCKED / DESPATCHED / COMPLETED / (any other) | UNKNOWN |

> `ORDER_CANCELED` is intentionally distinct from `CANCELLED` and maps to UNKNOWN
> — preserved from the original behaviour.

### 3.4 Fulfilment-Unit-ID Normalization

**Why it exists:** TDC is identified internally as SAP id "D001" but downstream
as "TDC".

```
fulfilmentUnitId = warehouseCode == ReflexConstants.TdcSapId
    ? ReflexConstants.TDCFulfilmentId   // "D001" → "TDC"
    : warehouseCode;                    // passthrough
```

| Input WarehouseCode | Output FulfilmentUnitId |
|---|---|
| "D001" | "TDC" |
| "TDC" | "TDC" |
| "ADC" | "ADC" |
| "PJCDC" | "PJCDC" |

### 3.5 Request Building & Publish
The `OrderTrackingCommonRequest` builder is documented in
[delta-towards-oms.md](shared/delta-towards-oms.md) (order-tracking request
section). Event-specific field mapping is in §7. Publishing is delegated to
[service-bus-publishing.md](shared/service-bus-publishing.md) — cached
`ServiceBusSender` to `order-tracking`, deterministic message id, correlation
headers, wrapped in the `service-bus-publish` Polly pipeline. This replaces the
previous commented-out `TODO` send, which is now fully implemented.

---

## 4. Calculation Logic

There is no quantity or inventory math in this event — the only computed value
is the boolean warehouse classification.

**Warehouse classification** (set-membership, case-insensitive):
```
isNotTDCorADC = !SpecialWarehouses.Contains(warehouseCode)
// SpecialWarehouses = { "D001", "TDC", "ADC" } using StringComparer.OrdinalIgnoreCase
```

| Input | In special set? | isNotTDCorADC |
|---|---|---|
| "D001" | yes | false |
| "TDC" | yes | false |
| "ADC" | yes | false |
| "PJCDC" | no | true |
| null / empty | — | validation failure (rejected) |

Using a static `HashSet<string>` with `OrdinalIgnoreCase` is both the efficient
(O(1) membership) and the correct (casing-tolerant) form, and removes the old
case-sensitivity edge case.

---

## 5. Database Documentation

**Not applicable — this event performs no database operations.**

This handler reads no Cosmos container and writes no Cosmos document. It has no
partition key, no ETag/Patch concern, and no `409`/`412` handling because it
never calls Cosmos. Its only I/O beyond consuming the inbound message is the
outbound Service Bus publish (§7) and best-effort cold-tier request audit
([archive-audit.md](shared/archive-audit.md)).

Consequently there is **no Cosmos connection string** in this event's
configuration (the stray Cosmos-connection-string reference in the previous
version was incorrect and has been removed — see §9).

---

## 6. State Changes & State Machine

This event holds no persistent state; the "state machine" is the in-memory
transform pipeline for a single message:

```
EVENT RECEIVED (OrderStatusChangedEvent)
   ↓
CLASSIFY warehouse → isNotTDCorADC
   ↓
SELECT reference id → orderId
   ↓
MAP status → OrderTrackingStatus
   ↓
NORMALIZE fulfilment unit id
   ↓
BUILD OrderTrackingCommonRequest
   ↓
PUBLISH → order-tracking (deterministic MessageId)
   ↓
Final: exactly one tracking request per distinct (WarehouseCode, OrderId, Status)
```

**Critical invariant:** a redelivered inbound message yields the same
deterministic outbound message id, so the `order-tracking` consumer de-duplicates
it — no duplicate tracking request.

---

## 7. API Documentation

### Kafka message contract
Topic `b2b.sales.OrderStatusChanged`, Avro payload mapped to
`OrderStatusChangedEvent`:

```json
{
  "channel": "B2B|B2C",
  "market": "UK|US|...",
  "orderId": "ORD-123456",
  "backOrderId": "BACKORD-789",
  "pickingRouteId": "ROUTE-456",
  "status": "CANCELLED|DELETED|...",
  "warehouseCode": "TDC|ADC|D001|PJCDC|...",
  "isReturn": false,
  "changeDate": "2024-01-15T10:30:00Z",
  "cancelReason": "Customer Request",
  "sourceOrderReferenceId": "EXT-REF-123"
}
```

### Service Bus message contract (inbound relay)
Queue `order-status-changed`, `ServiceBusRelayEnvelope` wrapping the event;
`SessionId = {WarehouseCode}:{OrderId}`; deterministic `MessageId`; correlation
headers per [service-bus-publishing.md](shared/service-bus-publishing.md).

### Output — order-tracking request
Queue `order-tracking`, `OrderTrackingCommonRequest`:

```json
{
  "referenceId": "ROUTE-789",
  "channel": "B2B",
  "backOrderId": "BACK-001",
  "fulfilmentUnitId": "TDC",
  "orderId": "ROUTE-789",
  "orderStatus": "CANCELLED",
  "lines": [ { "itemCode": null, "qty": 0 } ],
  "type": "B2B_ORDER_STATUS_CHANGED"
}
```

### Field mapping
| Input field | Transformation | Output field |
|---|---|---|
| Channel | `.ToString()` | Channel |
| OrderId / PickingRouteId | conditional selection (§3.2) | ReferenceId, OrderId |
| BackOrderId | direct copy | BackOrderId |
| Status | enum mapping (§3.3) | OrderStatus |
| WarehouseCode | normalization (§3.4) | FulfilmentUnitId |
| — | fixed | Type = B2B_ORDER_STATUS_CHANGED |
| — | fixed | Lines = [ empty OrderTrackingLine ] |

`Market`, `IsReturn`, `ChangeDate`, `CancelReason`, and `SourceOrderReferenceId`
are not mapped onto the tracking request (unchanged from the original behaviour).

### Validation
| Field | Rule | Handling |
|---|---|---|
| payload | not null / schema-valid | poison → DeadLettered |
| WarehouseCode | not null/empty | reject → DeadLettered |
| OrderId or PickingRouteId | resolved id not null/empty | reject → DeadLettered |
| Status | valid `StatusCode` enum | unmapped → UNKNOWN (by design) |

---

## 8. Error Handling & Retry Mechanisms

- **Validation / poison payload** → DeadLettered (hot-tier dead-letter container).
- **Missing/invalid WarehouseCode or reference id** → validation rejection →
  DeadLettered (no partial/invalid request is published).
- **Service Bus publish transient** → `service-bus-publish` Polly pipeline;
  exhausted retries surface as a processing failure.
- **`OperationCanceledException`** → Abandoned (redelivered up to
  `MaxDeliveryCount`).
- **Any other exception** → DeadLettered (`Reason` = type, `Description` =
  `ex.ToString()`).

There is **no Cosmos `412`/`ConcurrencyException` path** here because the event
does no Cosmos writes. Outcome mapping otherwise follows the definitive table in
[cosmos-idempotent-write.md](shared/cosmos-idempotent-write.md).

---

## 9. Security & Configuration

### Authentication
- **Service Bus** uses a **connection string** sourced from Azure Key Vault
  (delivered as a Kubernetes Secret); local dev uses the emulator / user-secrets.
  This is the deliberate documented standard — not Managed Identity / Workload
  Identity.
- **No Cosmos DB credential** is required or configured for this event — it
  performs no database access.

### Required Service Bus permissions
- `Listen` on `order-status-changed` (inbound).
- `Send` on `order-tracking` (outbound).

### Queue names (kebab-case, config-resolved)
| Queue | Old constant | Direction |
|---|---|---|
| `order-status-changed` | ORDER_STATUS_CHANGED_REFLEX_QUEUE_NAME | inbound (relay) |
| `order-tracking` | ORDER_TRACKING_QUEUE_NAME | outbound |

### Constants
| Constant | Value | Usage |
|---|---|---|
| `TdcSapId` | "D001" | classification + normalization |
| `TDCFulfilmentId` | "TDC" | normalization target |
| `ADCFulfilmentId` | "ADC" | classification |
| `EventType.B2B_ORDER_STATUS_CHANGED` | — | request type |

### Data protection
TLS in transit; encryption at rest; no secrets/keys logged; `CancelReason` and
`SourceOrderReferenceId` treated as sensitive and not emitted in logs.

---

## 10. Known Limitations & Future Improvements

### Current Limitations
- Only CANCELLED and DELETED statuses are mapped; all others become UNKNOWN by
  design.
- `Lines` is published as a single empty `OrderTrackingLine` (no line-level data
  on this event).
- `Market`, `IsReturn`, `ChangeDate`, `CancelReason`, `SourceOrderReferenceId`
  are not forwarded.

### Potential Improvements
- Expand status mapping if downstream requires more than CANCELLED/DELETED.
- Evaluate whether unmapped fields should be forwarded for downstream enrichment.

> The previous version listed "TODO: SendMessageAsync commented out", "no
> validation", "no error handling", and an unused `DurableTaskClient` as gaps.
> All are now resolved by design: the outbound send is implemented via the cached
> `ServiceBusSender` to `order-tracking` (§7/§9); input validation rejects
> null/invalid identifiers (§3, §7); message-outcome mapping governs error
> handling (§8); and there is **no** `DurableTaskClient`, `[ServiceBusTrigger]`,
> `function.json`, or `local.settings.json` — those Azure-Functions artefacts do
> not exist on the AKS pipeline.

---

## 11. Summary

`b2b.sales.OrderStatusChanged` processes OMS order-status transitions on the AKS
pipeline: consumes from Kafka, relays to the `order-status-changed` Service Bus
queue, classifies the warehouse, selects the reference id, maps the status,
normalizes the fulfilment-unit id, builds an `OrderTrackingCommonRequest`, and
publishes it to `order-tracking` via a cached `ServiceBusSender`.

**Key business logic:** TDC/ADC route on `PickingRouteId` while standard
warehouses route on `OrderId`; CANCELLED/DELETED map to their tracking
equivalents, everything else to UNKNOWN; SAP id `D001` normalizes to `TDC`.

**Database updates:** none — this is a transform-and-forward event with no Cosmos
access. Idempotency is provided by Kafka-side dedup plus a deterministic outbound
message id (downstream dedup), not by a Cosmos concurrency model.

**Risks & recommendations:** monitor dead-letter counts for null/invalid
identifiers and for unmapped statuses that may indicate a missing business rule.

---

**Document Version:** 2.0 (AKS / k8s)
**Status:** Regenerated
