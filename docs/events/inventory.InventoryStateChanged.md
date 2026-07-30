# inventory.InventoryStateChanged - Technical Documentation

## 1. Overview

### Purpose
`inventory.InventoryStateChanged` is a Kafka event that processes inventory
state-change events from the warehouse management system. It manages the
inventory lifecycle by handling state transitions (e.g. `PICKABLE → PREPARED`,
`→ HELD`), calculating B2B/B2C allocations and extensions, updating item stock
records, and synchronizing deltas across downstream systems (OMS, SAP, ICR)
via the Nexus producer.

### Business Objective
- Process inventory state changes (PICKABLE → PREPARED, HELD, etc.) in near
  real-time.
- Maintain accurate inventory records across B2B and B2C domains.
- Support B2C extended inventory allocation through intelligent segmentation.
- Synchronize inventory deltas with OMS, SAP, and external reporting (ICR).
- Archive historical inventory snapshots for audit and reconciliation.

### Scope
- Consumes `inventory.InventoryStateChanged` from Kafka, relays to Azure
  Service Bus, and processes it on a session-enabled Service Bus consumer.
- Processes pick and unpick events, and generic state-change segmentation.
- Performs inventory segmentation and B2C extension calculations.
- Persists item stock inventory to Cosmos DB via ETag-guarded **Patch**.
- Generates delta reports for OMS synchronization and ICR snapshots.
- Publishes downstream events to the `nexus-producer` queue and dispatches
  order-tracking requests to the `order-tracking` queue.

### High-Level Architecture

Matches the platform data flow in
[integration-resiliency.instructions.md](../ai/integration-resiliency.instructions.md):
a Kafka-to-Service-Bus relay hosted service, then a session-enabled Service Bus
consumer that calls the Application layer, which persists through the Cosmos DB
repository and archives through Blob Storage.

```
Kafka topic `inventory-events` (Type header: InventoryStateChanged, Avro)
                    ↓
   InventoryStateChangedConsumerHostedService (KafkaConsumerHostedServiceBase)
     - correlation id / dedup id / type headers read + logged
     - Nexus dedup check (IDeduplicationService, fail-open)
     - schema + dynamic validation
     - cold-tier request audit (unconditional)
                    ↓
   Azure Service Bus queue `inventory-state-changed`
   (session-enabled: SessionId = {FulfilmentId}:{ItemCode};
    message ID deterministic from the Kafka key — never a fresh GUID —
    for downstream dedup)
                    ↓
   InventoryStateChangedServiceBusHostedService (ServiceBusConsumerHostedService<InventoryStateChangedEvent>)
     - envelope + payload deserialize, dynamic validation, cold-tier audit
                    ↓
          IInventoryStateChangedHandler.HandleAsync
                    ↓
    ┌───────────────┬───────────────┬──────────────┬──────────────┬─────────────┐
    ↓               ↓               ↓              ↓              ↓             ↓
Pick Event     Unpick Event    Segmentation &   OMS Delta      ICR Snapshot   Order
Handler        Handler         Extended-State   (OMS)          (reporting)    Tracking
    ↓               ↓            transitions       ↓              ↓             ↓
IItemStockInventoryService → Cosmos DB (ETag-guarded Patch, re-read-and-reapply
on 412) + MessageArchive (Cosmos, optional Blob cold-tier mirror)
                    ↓
   nexus-producer queue (SAP/OMS/ICR) and order-tracking queue (Service Bus)
   via cached ServiceBusSender
```

Business logic never touches `CosmosClient`/`Container`/`ServiceBusSender`
directly — it goes through `IItemStockInventoryService` → the Cosmos repository
(§5) and through the application-layer publish abstraction (see
[shared/service-bus-publishing.md](shared/service-bus-publishing.md)), per
[cosmos-db.instructions.md](../ai/cosmos-db.instructions.md) §5 and
[dotnet-architecture-good-practices.instructions.md](../ai/dotnet-architecture-good-practices.instructions.md).

### Key Dependencies
- **`ItemStockInventoryRepository`** — core inventory (Cosmos, multi-container
  EDC/TDC/ADC/CAECOM/BRZ3PL, ETag-guarded; cosmos §5a/§9).
- **`ItemLevelSegmentationRepository`** / **`FulfilmentLevelSegmentationRepository`**
  — segmentation rules (Cosmos, read-only here).
- **`ItemStockInventoryExtendedRepository`** — non-standard state tracking (Cosmos).
- **`CountryRepository`** — country/market mapping (Cosmos, read-only).
- **`MessageArchiveRepository`** — snapshot archival (Cosmos + optional Blob).
- **Cached `ServiceBusSender`** — outbound Nexus / order-tracking publishing.
- **AutoMapper** — DTO mapping and transformation.
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
1. Incoming messages are valid `inventory.InventoryStateChanged` Avro objects
   deserialized to `InventoryStateChangedEvent`.
2. For pick/unpick events, inventory records exist in Cosmos (a miss is logged
   and the line is skipped, not fatal).
3. Negative quantities represent deductions; normalized per
   [inventory-formulas](shared/inventory-formulas.md).
4. Fulfilment location IDs map to known centers (TDC, EDC, ADC, CAECOM).
5. Country codes resolve from `CountryRepository` or fall back to `UNKNOWN`
   (see [country-code-lookup](shared/country-code-lookup.md)).
6. Inventory state transitions follow a predefined state machine (AVAILABLE,
   IN_TRANSIT, DAMAGED, QUARANTINE, etc.).
7. **Per-document consistency, not distributed transactions.** Item-line writes
   are not wrapped in a distributed transaction; correctness comes from
   per-document ETag-guarded Patch plus the §2 re-read/reapply loop, so a
   partial failure leaves each aggregate individually consistent and the message
   is safely retried (see
   [cosmos-idempotent-write](shared/cosmos-idempotent-write.md)).
8. **Processing is idempotent.** A deterministic document `Id` plus a
   deterministic Service Bus message ID and ETag-guarded Patch make redelivery a
   no-op, not a duplicate/double-count — this is the fix for the previous
   duplicate-entry / doubled-quantity behaviour (see
   [cosmos-idempotent-write](shared/cosmos-idempotent-write.md)).

---

## 2. End-to-End Flow

```
1. MESSAGE RECEPTION (Kafka consumer)
   ├─ InventoryStateChanged deserialized (Avro → InventoryStateChangedEvent)
   ├─ correlation/dedup/type headers logged; IDeduplicationService check (fail-open)
   ├─ schema + dynamic validation; cold-tier request audit
   └─ relay to Service Bus queue `inventory-state-changed`
        · SessionId = {FulfilmentId}:{ItemCode}
        · deterministic message ID from the Kafka key (never a fresh GUID)

2. SERVICE BUS CONSUMPTION
   ├─ envelope + payload deserialize, dynamic validation, cold-tier audit
   └─ IInventoryStateChangedHandler.HandleAsync(InventoryStateChangedEvent)
      · ReferenceId = InventoryStateChangedEvent.Id (deterministic, not a new GUID)

3. ITEM LINE ITERATION
   For each ItemLine in event.ItemLines:
   ├─ build deterministic uniqueIdentifier (ItemCode, LineNo, ReferenceId [+ OrderId])
   └─ build the item-stock request model

   4. EVENT TYPE CLASSIFICATION
      ├─ Pick   IF FromState=(AVAILABLE,PICKABLE) AND ToState=(AVAILABLE,PREPARED)
      ├─ Unpick IF FromState=(AVAILABLE,PREPARED) AND ToState=(AVAILABLE,HELD|PICKABLE)
      └─ else → generic state change (segmentation + extended-state)

   5. PICK EVENT (see §3.1)
      ├─ fetch ItemStockInventory; archive previous (archive-audit.md)
      ├─ B2B: Increment(B2BAllocated, -qty), Increment(B2BPrepared, +qty); recalc extension if IsExtended
      ├─ B2C: Increment(B2CPrepared, +qty); consume B2CAllocated or B2B share if extended
      └─ PERSIST via ETag-guarded Patch (Increment/Set), 412 re-read/reapply loop; archive new

   6. UNPICK EVENT (see §3.2)
      ├─ fetch; archive previous
      ├─ DGP: Increment(B2BPrepared, -qty); recalc extension if IsExtended
      └─ PERSIST via ETag-guarded Patch; archive new

   7. GENERIC STATE CHANGE (see §3.3–§3.5)
      ├─ B2B adjusted/moved (SAP) → publish Inventory_B2BInventoryAdjustedOrMoved → nexus-producer
      ├─ segmentation (AVAILABLE+PICKABLE): 3PL → fulfilment-level; WH → item-level (IsExtended) else fulfilment-level
      │   · delta = currB2CAVL − prevB2CAVL; IsB2CChanged; Patch Increment/Set; archive before/after
      └─ extended-state transitions (other states): TO-state create-if-missing + Patch Increment(+);
          FROM-state validate-sufficient + Patch Increment(−)

8. OMS DELTA (ENABLE_DELTA_TOWARDS_OMS[/_3PL] AND IsB2CChanged) — delta-towards-oms.md
   └─ resolve CountryCode (country-code-lookup.md); publish Inventory_B2CInventoryAdjusted → nexus-producer

9. ICR SNAPSHOT (ENABLE_SNAPSHOT_FOR_ICR) — icr-snapshot.md
   └─ publish Inventory_OmniInventoryAvailabilityReported → nexus-producer

10. ORDER TRACKING (cross-item, once per message, if isPickEvent OR isUnpickEvent)
    └─ build OrderTrackingCommonRequest; publish → order-tracking (delta-towards-oms.md)

11. OUTCOME
    └─ no exception → Completed; ConcurrencyException/OperationCanceled → Abandoned;
       any other → DeadLettered (see cosmos-idempotent-write.md)
```

### Key State Transitions in Database

| Entity | From | To | Trigger | Patch action |
|--------|------|----|---------|--------------|
| ItemStockInventory | B2BAllocated | B2BPrepared | B2B Pick | Increment(B2BAllocated,−qty), Increment(B2BPrepared,+qty) |
| ItemStockInventory | B2CAllocated | B2CPrepared | B2C Pick | Increment(B2CPrepared,+qty); Increment(B2CAllocated,−) or B2B share |
| ItemStockInventory | B2BPrepared | B2BAllocated | Unpick (DGP) | Increment(B2BPrepared,−qty) (reverse of pick) |
| ItemStockInventory | B2CAVL | (recalc) | Extension update | Set(B2CExtended/B2CAVL) from formula helper |
| ItemStockInventoryExtended | none | created | State change | create-if-missing (deterministic Id, 409-as-applied) |
| ItemStockInventoryExtended | Qty | Qty±delta | Inbound/Outbound | Increment(Qty, ±inboundQty) |

### Data Flow Through Layers
`Kafka → KafkaConsumerHostedServiceBase → Service Bus (inventory-state-changed) →
ServiceBusConsumerHostedService → IInventoryStateChangedHandler → pick/unpick/
segmentation helpers → IItemStockInventoryService → Cosmos repository (Patch/ETag)
+ archive → ServiceBusSender (nexus-producer / order-tracking)`.

---

## 3. Detailed Business Logic

### 3.1 Pick Event Logic

**Purpose**: Process the allocation-to-prepared transition when inventory is
prepared for shipment.

**Trigger Condition**:
```
FromState = (AVAILABLE, PICKABLE) AND ToState = (AVAILABLE, PREPARED)
```

**B2B Pick Flow**:
1. Fetch inventory by ItemCode, Hallmark, FulfilmentCode, CountryOfOrigin.
2. **Validation**: if not found → log warning, return null (line skipped).
3. Archive current state ([archive-audit.md](shared/archive-audit.md)).
4. **Calculation** (applied as Patch `Increment`, not read-modify-write):
   - `B2BAllocated -= PickQuantity`
   - `B2BPrepared  += PickQuantity`
5. **Validation**: if `B2BAllocated` would go negative → log warning, cap at 0.
6. **Extension check**: if `IsExtended`, recalculate B2C impact via
   `CalculateB2CExtensionAsync()` against the previous `B2CAVL` (see
   [b2c-extension-calculation.md](shared/b2c-extension-calculation.md)).
7. Archive updated state.
8. Persist via ETag-guarded Patch; `412` → §2 re-read/reapply loop.
9. Return delta info (if B2C changed) for OMS synchronization.

**B2C Pick Flow**:
1. Fetch inventory by ItemCode, Hallmark, FulfilmentCode, CountryOfOrigin.
2. **Validation**: if not found → log warning, return null.
3. Archive current state.
4. Initialize `B2BPrepared`/`B2CPrepared` to 0 if null.
5. **Two scenarios**:

   **Scenario A — Sufficient B2C allocation** (`B2CAllocated >= PickQuantity`):
   - `B2CPrepared += PickQuantity`
   - `B2CAllocated -= PickQuantity`
   - If `B2CAllocated` would go negative → log warning, cap at 0.

   **Scenario B — Insufficient B2C allocation** (`B2CAllocated < PickQuantity`):
   - **If NOT Extended**: log error, return null (fail the operation).
   - **If Extended**:
     - Overage: `B2BStock = PickQuantity - B2CAllocated`
     - Consume B2B share: `B2BUsedShare -= B2BStock`
     - Set `B2CAllocated = 0`
     - `B2CPrepared += PickQuantity`
     - If `B2BUsedShare` would go negative → log warning, return null.
     - Recalculate B2C available via `CalculateB2CExtensionAsync()` against the
       previous `B2CAVL`.

6. Archive updated state.
7. Persist via ETag-guarded Patch; `412` → §2 re-read/reapply loop.
8. Return delta info.

**Error Scenarios**:
- Inventory record missing → logged, bypassed (returns null).
- Negative quantities → logged, capped at 0.
- B2C pick without allocation and not extended → operation fails.
- B2B share insufficient for B2C overage → logged, returns null.

### 3.2 Unpick Event Logic

**Purpose**: Reverse a pick operation, returning inventory from prepared back to
allocated.

**Trigger Condition**:
```
(FromState = (AVAILABLE, PREPARED) AND ToState = (AVAILABLE, HELD))
OR
(FromState = (AVAILABLE, PREPARED) AND ToState = (AVAILABLE, PICKABLE))
```

**Processing**:
1. Fetch inventory by ItemCode, Hallmark, FulfilmentCode, CountryOfOrigin.
2. **Validation**: if not found → log warning, return null.
3. Archive current state.
4. Initialize `B2CPrepared` to 0 if null.
5. **Type-specific logic**:
   - **If Type == DGP** (Demand Generation Product):
     - Validate `B2BPrepared > 0`, else log warning and return null.
     - `B2BPrepared -= UnpickQuantity` (Patch `Increment(-qty)`).
   - **Else**: log error for invalid type, return null.
6. **Extension recalculation**: if `IsExtended`, recalculate B2C impact via
   `CalculateB2CExtensionAsync()` against the previous `B2CAVL`.
7. Archive updated state.
8. Persist via ETag-guarded Patch; `412` → §2 re-read/reapply loop.
9. Return delta info.

**Reverse Semantics**:
- Unpick reverses the pick operation completely.
- B2C extension is recalculated to reflect availability after unpick.
- Used primarily for order cancellations or prep-to-hold transitions.

### 3.3 Inventory Segmentation & Extension Logic

**Purpose**: Distribute new inventory across B2B and B2C domains based on
fulfilment-level or item-level rules. Full algorithm in
[segment-inventory.md](shared/segment-inventory.md); event-specific summary:

**Trigger Condition**:
```
(FromState = AVAILABLE AND FromStatus = PICKABLE)
OR (ToState = AVAILABLE AND ToStatus = PICKABLE)
```

**Segmentation by Location Type**:

| Location Type | Strategy | Data Source |
|---------------|----------|-------------|
| THIRD_PARTY_LOGISTICS (3PL) | Fulfilment-level B2C segmentation | FulfilmentLevelSegmentationRepository |
| WAREHOUSE (default) | Item-level (if active) else fulfilment-level | ItemLevelSegmentationRepository → FulfilmentLevelSegmentationRepository |

**Algorithm**:
```
1. Fetch ItemStockInventory (create-if-missing: deterministic Id, 409-as-applied)
2. inboundQty = signed normalization (inventory-formulas.md)
3. Validate: inboundQty < 0 AND inventory was just created → fail (cannot negate empty)
4. Capture previous B2CAVL / B2COrg for delta calculation
5. Apply segmentation:
   - 3PL → DoFulfilmentLevelB2CSegmentation(inboundQty, inventory)
   - else fetch item-level rules:
       IF rules exist AND IsActive → IsExtended = true; DoItemLevelExtension(inboundQty, ecomShare%, inventory)
       ELSE → DoFulfilmentLevelSegmentation(inboundQty, inventory)
6. delta = currentB2CAVL − previousB2CAVL; IsB2CChanged = (delta ≠ 0)
7. Archive before/after
8. PERSIST via ETag-guarded Patch (Increment for quantities, Set for IsExtended);
   412 → §2 re-read/reapply loop
```

- **Fulfilment-level** (default): uniform B2C allocation % across items; used
  when no item-level rules exist.
- **Item-level** (advanced): item-specific B2C %, higher priority than
  fulfilment-level, supports brand/category strategies and storage-leverage %;
  sets `IsExtended`.

### 3.4 B2C Extension Calculation

**Purpose**: Allocate B2B inventory to B2C when B2C demand exceeds formal
allocation. Centralized in
[b2c-extension-calculation.md](shared/b2c-extension-calculation.md) and
[inventory-formulas.md](shared/inventory-formulas.md); event-specific detail:

**When Triggered**:
- B2B pick on an extended item.
- B2C pick without sufficient allocation (requires extension).
- Unpick on extended inventory.
- Segmentation with item-level rules and `IsExtended`.

**Calculation Formula**:
```
B2CExtended = CalculateActualB2BAvailable(inventory) = B2BAVL - B2BAllocated - B2BUsedShare
B2CAVL_new  = CalculateB2CAvl(inventory)            = B2COrg + B2CExtended
DeltaToOMS  = B2CAVL_new - B2CAVL_prev
```

**Variables**:
- **B2BAVL**: total B2B available inventory
- **B2BAllocated**: B2B inventory reserved/allocated
- **B2BUsedShare**: B2B inventory consumed to fulfil B2C demand
- **B2COrg**: original B2C-specific allocation
- **B2CExtended**: B2B inventory temporarily allocated to B2C
- **B2CAVL**: total B2C available (original + extended)

**Boundary Conditions**:
- `B2CExtended >= 0` (capped at 0) and `<= (B2BAVL - B2BAllocated)`.
- `B2CAVL` recalculated only when extension changes.
- Missing storage-leverage defaults to 0.

**Worked Example**:
```
Scenario: B2C pick on extended item requires B2B share
Input:
  PickQuantity = 100, B2CAllocated = 60, B2BAVL = 500, B2BAllocated = 200,
  B2BUsedShare = 0 (before), B2COrg = 60, B2CAVL_prev = 60
Processing:
  Step 1: B2CAllocated (60) < PickQuantity (100) → use extension
  Step 2: B2BStock required = 100 - 60 = 40
  Step 3: B2BUsedShare = 0 + 40 = 40
  Step 4: B2CAllocated = 0
  Step 5: B2CPrepared = 0 + 100 = 100
  Step 6: B2CExtended = 500 - 200 - 40 = 260
  Step 7: B2CAVL_new = 60 + 260 = 320
  Step 8: DeltaToOMS = 320 - 60 = +260
Result: OMS is notified that B2C available increased by 260 due to extension
```

### 3.5 Extended Inventory Segmentation

**Purpose**: Track inventory in non-standard states (DAMAGED, QUARANTINE,
IN_TRANSIT, etc.) separately for compliance and reporting.

**Trigger Condition**:
```
FromState != (AVAILABLE, PICKABLE) OR ToState != (AVAILABLE, PICKABLE)
```

**TO-state handling** (`ToState != (AVAILABLE, PICKABLE)`):
```
1. Fetch ItemStockInventoryExtended for (ItemCode, Hallmark, FulfilmentCode, COO, ToState, ToStatus)
2. If missing → create (deterministic Id; 409-as-applied) with Qty = inboundQty
3. Else → archive previous, Patch Increment(Qty, +inboundQty)
4. Archive updated record
```

**FROM-state handling** (`FromState != (AVAILABLE, PICKABLE)`):
```
1. Fetch ItemStockInventoryExtended for (ItemCode, Hallmark, FulfilmentCode, COO, FromState, FromStatus)
2. If exists AND Qty >= |inboundQty| → archive previous, Patch Increment(Qty, -|inboundQty|), archive updated
3. Else → log warning (insufficient quantity in extended state); skip (never negative)
```

**State Examples**:
| State | Status | Meaning |
|-------|--------|---------|
| AVAILABLE | PICKABLE | Ready for picking |
| AVAILABLE | PREPARED | Picked, awaiting shipment |
| AVAILABLE | HELD | Prepared but on hold |
| IN_TRANSIT | INTRANSIT | In shipment |
| DAMAGED | DAMAGED | Quality issues |
| QUARANTINE | QUARANTINE | Regulatory hold |

### 3.6 B2B Adjusted/Moved Event Publishing (SAP via Nexus)

**Purpose**: Notify SAP of inventory adjustments for master-data
synchronization. Builder, trigger conditions, and the SAE-2798/SAE-3032 fixes
are specified in [delta-towards-oms.md](shared/delta-towards-oms.md);
event-specific inputs below.

**Trigger Condition**:
```
NOT a Pick/Unpick event AND one of:
  - ENABLE_DELTA_TOWARDS_SAP AND Location != EDC AND Location != ADC
  - ENABLE_DELTA_TOWARDS_AX12_3PL AND Location == CAECOM
  - ENABLE_ADC_DELTA_TOWARDS_AX12 AND Location == ADC
```

**Processing**:
1. Map `InventoryStateChangedEvent` → `B2BInventoryAdjustedOrMovedEvent`.
2. **SAE-2798 fix**: for non-`B2B_INVENTORY_ADJUSTED` types, if
   `FromState.State == ToState.State` and neither is AVAILABLE → skip publishing
   (invalid transition).
3. **SAE-3032 fix**: if `FromState.State != AVAILABLE` → `FromState.Status =
   UNKNOWN`; if `ToState.State != AVAILABLE` → `ToState.Status = UNKNOWN`.
4. **Quantity normalization**: negative adjustment lines converted to positive
   (`Math.Abs`) per [inventory-formulas.md](shared/inventory-formulas.md).
5. **Publish**: wrap in `NexusProducerRequest`
   (type `Inventory_B2BInventoryAdjustedOrMoved`) and publish to
   `nexus-producer` via the cached `ServiceBusSender`
   ([service-bus-publishing.md](shared/service-bus-publishing.md)).

**Error Scenarios**:
- `ReferenceId` missing → derived deterministically from the source event (never
  a fresh GUID) and logged.
- `FromState == ToState` and neither AVAILABLE → skip (invalid).
- State not AVAILABLE but Status not UNKNOWN → normalize to UNKNOWN.

### 3.7 OMS Delta Synchronization

**Purpose**: Notify OMS of B2C availability changes for fulfilment decisions.
Full builder in [delta-towards-oms.md](shared/delta-towards-oms.md).

**Trigger Condition**:
```
result != null AND result.IsB2CChanged AND ENABLE_DELTA_TOWARDS_OMS
(with LocationType-specific feature flag checks)
```

**Request Structure** (`DeltaTowardsOmsEventRequest`):
```
ReferenceId      : deterministic from source event (never Guid.NewGuid())
ProductId        : ItemCode
Location         : (Id, Type) from event
Reason           : ReasonCode.ADJUSTMENT
AdjustmentDate   : caller-supplied UTC
ProductUnits     : "N/A"
Market           : CountryCode (country-code-lookup.md)
QuantityDetails  : [ { CountryOfOrigin, Hallmarking, Quantity = result.DeltaTowardsOMS (signed),
                       State = (AVAILABLE, PICKABLE), ReasonTexts = [] } ]
```

**Publishing**:
1. Resolve `CountryCode` from `CountryRepository` by FulfilmentId; fallback to
   `CountryCode.UNKNOWN` on miss/parse failure
   ([country-code-lookup.md](shared/country-code-lookup.md)).
2. Build `DeltaTowardsOmsEventRequest` with the deterministic `ReferenceId`, the
   calculated signed delta, and `State/Status = (AVAILABLE, PICKABLE)` for OMS.
3. Wrap in `NexusProducerRequest` (type `Inventory_B2CInventoryAdjusted`).
4. Publish to `nexus-producer` via the cached `ServiceBusSender`.

**Failure Handling**:
- Country lookup fails → fallback to `CountryCode.UNKNOWN`.
- B2C not changed → skip publishing (conserve queue traffic).
- Feature flag disabled → skip with an information log.

### 3.8 Inventory Comparison Report (ICR)

**Purpose**: Snapshot inventory state for Inventory Comparison Reports and
auditing. Full snapshot builder in
[icr-snapshot.md](shared/icr-snapshot.md).

**Trigger Condition**: `ENABLE_SNAPSHOT_FOR_ICR = true`.

**Processing**:
1. Fetch `ItemStockInventory` (if missing → skip snapshot for the item).
2. Build snapshot of all allocation states:
   - B2B Available (AVAILABLE, PICKABLE)
   - B2C Available (uses `B2COrg` if `IsExtended`, else `B2CAVL`)
   - B2B Prepared / B2C Prepared (AVAILABLE, PREPARED)
3. Map to `OmniInventoryAvailabilityReported` (ProductId, CountryOfOrigin,
   Hallmarking, Location [3PL detection for CAECOM], QuantityDetails
   `[B2B_AVL, B2C_AVL, B2B_PREP, B2C_PREP]`, `ReportDate`, `ProductUnits`).
4. Wrap in `NexusProducerRequest` (type
   `Inventory_OmniInventoryAvailabilityReported`).
5. Publish to `nexus-producer` via the cached `ServiceBusSender`.

**Data Accuracy**:
- Uses `B2COrg` if `IsExtended` (reflects original allocation under extension).
- Uses `B2CAVL` if not extended (reflects effective availability).

### 3.9 Order Tracking

**Purpose**: Notify the order-tracking system of pick/unpick progress
(cross-item, once per message).

**Trigger Condition**: `isPickEvent OR isUnpickEvent`.

**Processing**:
1. Build `OrderTrackingCommonRequest` from the message (see the builder in
   [delta-towards-oms.md](shared/delta-towards-oms.md)).
2. Publish to `order-tracking` via the cached `ServiceBusSender`
   ([service-bus-publishing.md](shared/service-bus-publishing.md)).
3. A publish failure is logged and does not fail the message's inventory
   outcome (best-effort side channel).

---

## 4. Calculation Logic

All quantity math is centralized in
[inventory-formulas.md](shared/inventory-formulas.md) and
[b2c-extension-calculation.md](shared/b2c-extension-calculation.md). Increments
are applied with `PatchOperation.Increment`, never read-modify-write-replace.

### Formula 1 — Inbound Quantity
```
inboundQty = Convert.ToInt32(MoveSign + Quantity.ToString())   // signed
```
- `MoveSign` = `"+"`, `"-"`, or empty; `""`/`"+"` = addition, `"-"` = deduction.
- Examples: `("", 100) → +100`, `("-", 50) → -50`, `("+", 75) → +75`.
- Integer precision; result can be zero for balanced transfers.

### Formula 2 — B2C Available
```
B2CAVL = B2COrg + B2CExtended
```
- `B2COrg`/`B2CExtended` default to 0 if null; `B2CAVL >= 0`.
- If not extended, `B2CExtended = 0` so `B2CAVL = B2COrg`.

**Worked Example**: `B2COrg = 200, B2CExtended = 0 → B2CAVL = 200`.

### Formula 3 — B2CExtended
```
B2CExtended = B2BAVL - B2BAllocated - B2BUsedShare
```
- Capped at 0 (never negative) and `<= B2BAVL`.
- `(B2BAVL - B2BAllocated)` is the maximum available for sharing.

**Worked Examples**:
```
Ex1: B2BAVL=500, B2BAllocated=200, B2BUsedShare=50 → B2CExtended = 250
Ex2: B2BAVL=200, B2BAllocated=200, B2BUsedShare=0  → B2CExtended = 0 (none available)
```

### Formula 4 — Delta Towards OMS
```
DeltaTowardsOMS = B2CAVL_new - B2CAVL_previous
```
- Signed; `0` means no OMS notification needed (only publish if `delta != 0`).

**Worked Example**:
```
B2CAVL_prev = 100; B2C pick triggers extension recalculation; B2CAVL_new = 250
DeltaTowardsOMS = 250 - 100 = +150  → OMS notified of +150 units now available
```

| Previous | Current | Delta | IsB2CChanged |
|---|---|---|---|
| 100 | 320 | +220 | true |
| 100 | 75 | −25 | true |
| 100 | 100 | 0 | false |

---

## 5. Database Documentation

All Cosmos access follows
[cosmos-db.instructions.md](../ai/cosmos-db.instructions.md) and
[cosmos-idempotent-write.md](shared/cosmos-idempotent-write.md): deterministic
`Id`, point reads within a partition, `Create` with `409 Conflict` →
treat-as-applied, and `PatchAsync` with `IfMatchEtag` (≤10 ops) — **never
last-write-wins** on any quantity field.

### 5.1 ItemStockInventory (Cosmos, multi-container per fulfilment code)

**Purpose**: Central inventory record tracking all allocation states across B2B
and B2C domains (EDC/TDC/ADC/CAECOM/BRZ3PL containers per cosmos §5a).

- **Partition key** `Category` = composite
  `FulfilmentId:ItemCode:Hallmark:CountryOfOrigin`.
- **Read**: `GetAsync(id, category)` — point read within one partition
  (equivalent to the old lookup by ItemCode + Hallmark + FulfilmentCode + COO).
- **Create (first write)**: deterministic `Id` (never `Guid.NewGuid()`);
  `409 Conflict` → return existing (redelivery no-op). Quantity fields default
  to 0; `IsExtended` false; timestamps caller-supplied UTC.
- **Update**: `PatchAsync` with `IfMatchEtag`, `PatchOperation.Increment` for
  B2B/B2C quantities and `.Set` for flags (`IsExtended`) and `ModifiedUtc`,
  ≤10 ops. `412` → `ConcurrencyException` → §2 re-read/reapply loop (max 3).

**Fields & how derived**:
| Field | How derived |
|---|---|
| ItemCode / Hallmark / FulfilmentId / COO | identity (create only) |
| B2BAVL / B2BAllocated / B2BPrepared / B2BUsedShare | Patch Increment per event |
| B2CAVL / B2COrg / B2CPrepared / B2CAllocated / B2CExtended | segmentation + extension helpers → Patch Increment/Set |
| InternalHallmarkAllocated / InTransit / PSC | read (not modified here) |
| IsExtended | item-level rule active → Patch Set |
| ModifiedUtc | caller-supplied UTC → Patch Set |

**Update scenarios** (all applied as ETag-guarded Patch):
1. **B2B Pick** — Increment(B2BAllocated,−qty), Increment(B2BPrepared,+qty);
   Set(B2CExtended/B2CAVL) if `IsExtended`.
2. **B2C Pick** — Increment(B2CPrepared,+qty); Increment(B2CAllocated,−qty) or
   consume B2B share (Increment(B2BUsedShare,−overage), Set(B2CAllocated,0)) if
   extended; recalc B2CExtended/B2CAVL.
3. **Unpick (DGP)** — Increment(B2BPrepared,−qty); recalc extension if
   `IsExtended`.
4. **Segmentation** — Increment B2B/B2C availability per rules; Set(IsExtended)
   when item-level rules active; recalc B2CExtended.

### 5.2 ItemStockInventoryExtended (Cosmos)

**Purpose**: Track inventory in non-standard states for compliance.

- Composite key includes State/Status; partition per cosmos §5a.
- **TO-state (inbound)**: create-if-missing (deterministic Id, 409-as-applied)
  then Patch `Increment(Qty, +inboundQty)`.
- **FROM-state (outbound)**: only when existing `Qty >= |inboundQty|`; Patch
  `Increment(Qty, -|inboundQty|)`. Insufficient → warning, skip (never
  negative).

### 5.3 ItemLevelSegmentation / FulfilmentLevelSegmentation (Cosmos, read-only)

- Point reads by category; supply `EcomShare`, `IsActive`,
  `StoreLeveragePercentage`, `IsOMNI`, effective/expiry dates.
- Item-level rules take priority; fulfilment-level are the fallback default.
- **Item-level segmentation update**: only when NOT a TDC location, a bounded
  Patch (`Set`) syncs `IsExtended`/leverage after extension calculation; rules
  themselves are owned by a separate operational process (not created here).

### 5.4 Country (Cosmos, read-only)

- Point read by FulfilmentId → market/`CountryCode` for OMS events; fallback to
  `UNKNOWN` (see [country-code-lookup.md](shared/country-code-lookup.md)).

### 5.5 Archive (MessageArchive)

- Before/after snapshots of `ItemStockInventory` /
  `ItemStockInventoryExtended` / item-level segmentation via
  [archive-audit.md](shared/archive-audit.md); best-effort — an archive failure
  is logged and does not fail the message.

### 5.6 Transaction Flow & Concurrency

- **Transaction boundary**: one Service Bus message; item lines processed
  sequentially, each aggregate written independently.
- **Concurrency control**: per-document **ETag optimistic concurrency**; `412` →
  `ConcurrencyException` → §2 bounded re-read/reapply loop (max 3). There is
  **no last-write-wins** and no distributed transaction — correctness comes from
  deterministic Id + ETag Patch, which is the fix for concurrent-update
  double-counting.
- **Retry**: Cosmos `429` handled by the SDK retry; message-level failures are
  retried by Service Bus up to `MaxDeliveryCount`, then dead-lettered.

---

## 6. State Changes & State Machine

```
INVENTORY LIFECYCLE (illustrative):

Initial:  ItemCode "ABC123", Hallmark 22K, Location TDC
          B2BAVL 1000, B2CAVL 500, B2BAllocated 200, B2CAllocated 400, IsExtended false

  ↓ PICK (B2B, Qty=100)      → Increment(B2BAllocated,−100), Increment(B2BPrepared,+100)
     B2BAllocated 100, B2BPrepared 100, B2CAVL 500 (unchanged if not extended)

  ↓ UNPICK (DGP, Qty=50)     → Increment(B2BPrepared,−50) [B2BAllocated returns to 150]
     B2BAllocated 150, B2BPrepared 50, B2CAVL recalculated if extended

  ↓ INBOUND SEGMENTATION (Qty=+200)
     B2BAVL 1200, B2CAVL 600 (per segmentation rule), IsExtended true (item-level rule)

  ↓ B2C PICK WITH EXTENSION (Qty=150, within allocation)
     B2CAllocated 250, B2CPrepared 150, B2BUsedShare 0, B2CAVL 600

  ↓ B2C PICK EXCEEDING ALLOCATION (Qty=350)
     B2CAllocated 0, B2CPrepared 350, B2BUsedShare 100 (overage), B2CExtended & B2CAVL recalculated
```

### State Machine Rules

| From | To | Event | Valid |
|------|----|-------|-------|
| (AVAILABLE, PICKABLE) | (AVAILABLE, PREPARED) | PICK | ✓ |
| (AVAILABLE, PREPARED) | (AVAILABLE, HELD) | UNPICK | ✓ |
| (AVAILABLE, PREPARED) | (AVAILABLE, PICKABLE) | UNPICK | ✓ |
| (AVAILABLE, PICKABLE) | (IN_TRANSIT, INTRANSIT) | SHIP | ✓ generic |
| (IN_TRANSIT, INTRANSIT) | (AVAILABLE, PICKABLE) | RECEIVE | ✓ generic |
| (AVAILABLE, PICKABLE) | (DAMAGED, DAMAGED) | DAMAGE | ✓ generic |
| (AVAILABLE, PICKABLE) | (QUARANTINE, QUARANTINE) | QUARANTINE | ✓ generic |
| (Any, Any) | (Any, Any) | other | ℹ️ logged for visibility |

### Critical Invariants (Must Always Hold)

1. **Quantity non-negativity**: `B2BAllocated, B2BPrepared, B2CAllocated,
   B2CPrepared, B2BUsedShare >= 0` (violations logged, capped at 0).
2. **B2B share conservation**: `B2BAVL >= B2BAllocated + B2BPrepared +
   B2BUsedShare` (violation may fail the operation, especially B2C overage).
3. **B2C availability**: `B2CAVL >= B2CAllocated + B2CPrepared` (held via
   extension logic).
4. **Extension-flag consistency**: `IsExtended == true ⇒ B2CExtended > 0`;
   `IsExtended == false ⇒ B2CExtended == 0`.
5. **Idempotency**: a redelivered message produces no additional mutation
   (deterministic Id + ETag Patch).

---

## 7. API Documentation

### Kafka message contract

Topic `inventory-events`, `Type` header `InventoryStateChanged`, Avro payload
mapped to `InventoryStateChangedEvent`:

```json
{
  "id": "UUID",
  "referenceId": "ORDER-12345",
  "type": "PICKEDB2B | PICKEDB2C | TRANSFER",
  "channel": "OMS | EXTERNAL_SYSTEM",
  "location": { "id": "TDC | EDC | ADC | CAECOM", "type": "WAREHOUSE | THIRD_PARTY_LOGISTICS" },
  "fromState": { "state": "AVAILABLE | IN_TRANSIT | DAMAGED | QUARANTINE", "status": "PICKABLE | PREPARED | HELD | UNKNOWN | INTRANSIT" },
  "toState":   { "state": "AVAILABLE | IN_TRANSIT | DAMAGED | QUARANTINE", "status": "PICKABLE | PREPARED | HELD | UNKNOWN | INTRANSIT" },
  "moveSign": "+ | - | ",
  "itemLines": [
    { "productId": "ITEM-CODE-123", "lineNum": "1", "quantity": 100, "countryOfOrigin": "IN", "hallmarking": "22K | 18K | 24K" }
  ],
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Headers** (standard Kafka headers): `ReceivedTime`, `CorrelationId`,
`EventType`, `DeDuplicationId` (derived from payload — feeds the deterministic
Service Bus message ID), `EventKey`.

### Service Bus message contract

Queue `inventory-state-changed`, `ServiceBusRelayEnvelope` wrapping the event;
`SessionId = {FulfilmentId}:{ItemCode}`; deterministic `MessageId`; correlation
headers per [service-bus-publishing.md](shared/service-bus-publishing.md).
Processing is fire-and-forget with side effects (DB updates, archives,
downstream publishes). Outcome mapping per §8.

### Validation

| Field | Rule | Handling |
|---|---|---|
| payload | not null / schema-valid | poison → DeadLettered |
| ItemCode | not null / not empty | reject line |
| Quantity | integer >= 0 | signed parse (inventory-formulas.md) |
| Location.Id | resolvable fulfilment centre | fallback `CountryCode.UNKNOWN` |
| From/To State/Status | valid enum | reject invalid |
| Hallmarking / CountryOfOrigin | valid / parseable enum | fallback where documented |

**Application-level errors** (logged, not returned to a caller):
| Error | Scenario | Handling |
|---|---|---|
| MissingItemStockInventoryException | inventory not found | log, skip line (returns null) |
| InvalidItemStockInventoryQtyException | quantity would go negative | log, cap at 0 |
| InvalidDataException | invalid event type/state | log, skip operation |
| InvalidExtendedItemStockInventoryQtyException | extended underflow | log, skip update |
| CountryCode.UNKNOWN | country lookup miss | fallback UNKNOWN |

---

## 8. Error Handling & Retry Mechanisms

- **Validation / poison payload** → DeadLettered (hot-tier dead-letter container).
- **Cosmos 412 (ETag)** → `ConcurrencyException` → §2 re-read/reapply loop (≤3);
  if exhausted → Abandoned (redelivered up to `MaxDeliveryCount`).
- **Cosmos 429** → Cosmos SDK retry (`MaxRetryAttemptsOnRateLimitedRequests`).
- **Service Bus publish transient** → `service-bus-publish` Polly pipeline.
- **`OperationCanceledException`** → Abandoned.
- **Any other exception** → DeadLettered (`Reason` = type name, `Description` =
  `ex.ToString()`).
- **Application rejections** (missing inventory, invalid/underflow quantity) →
  logged; that line is skipped without failing the whole message.
- **Best-effort side channels** (archive, ICR, order-tracking publish) → a
  failure is logged and does not fail the inventory outcome.

Outcome mapping is the definitive table in
[cosmos-idempotent-write.md](shared/cosmos-idempotent-write.md).

---

## 9. Security & Configuration

### Authentication

> **Deviation from documented standards — flagged per
> [CLAUDE.md](../../CLAUDE.md) precedence rules.**
> [cosmos-db.instructions.md](../ai/cosmos-db.instructions.md) §1/§14 and
> [engineering-standards.instructions.md](../ai/engineering-standards.instructions.md) §6
> specify connection strings sourced from **Azure Key Vault** (delivered as a
> **Kubernetes Secret**) for Cosmos DB, Service Bus, and Blob Storage in every
> non-local environment, with the emulator / `.NET user-secrets` for local dev
> only — **not** Managed Identity / Workload Identity (a deliberate,
> explicitly-directed standards change). This service follows that documented
> standard; the connection-string mechanism is intentional and must not be
> "corrected" to Managed Identity.

- **Never commit** a connection string value to source control or
  `appsettings.json`; the value is a secret sourced from a Kubernetes Secret
  (Key Vault-backed).
- TLS / AMQPS-over-TLS applies at the protocol level regardless of credential
  type (engineering-standards §6).
- Rotating a connection string requires redeploying the Pods that hold the
  Secret (or a Secret-reload mechanism).

### Feature flags
| Flag | Purpose |
|---|---|
| ENABLE_DELTA_TOWARDS_SAP | B2B SAP adjusted/moved events (warehouse) |
| ENABLE_DELTA_TOWARDS_AX12_3PL | B2B SAP events for CAECOM (3PL) |
| ENABLE_ADC_DELTA_TOWARDS_AX12 | ADC-specific SAP events |
| ENABLE_DELTA_TOWARDS_OMS | OMS B2C notifications (warehouse) |
| ENABLE_DELTA_TOWARDS_OMS_3PL | OMS B2C notifications (3PL) |
| ENABLE_SNAPSHOT_FOR_ICR | ICR snapshots |

### Queue names (kebab-case, config-resolved)
| Queue | Old constant | Direction |
|---|---|---|
| `inventory-state-changed` | INVENTORY_STATE_CHANGED_REFLEX_QUEUE_NAME | inbound (relay) |
| `nexus-producer` | NEXUS_PRODUCER_QUEUE_NAME | outbound (SAP / OMS / ICR) |
| `order-tracking` | ORDER_TRACKING_QUEUE_NAME | outbound (order tracking) |

### Data protection
TLS in transit; encryption at rest; no secrets/keys logged. Archived payloads
carry business data only.

---

## 10. Known Limitations & Future Improvements

### Current Limitations
- Integer quantities only (no fractional units).
- Segmentation rules and country codes read per line; may be cached (below).
- Extended FROM-state with insufficient quantity is skipped with a warning
  rather than reconciled.
- Item lines processed sequentially within a message (per-aggregate ordering
  preserved via the session).

### Potential Improvements
- Cache country codes and segmentation rules per process to cut Cosmos reads.
- Batch downstream publishes where a line produces multiple events.
- Evaluate bounded parallel line processing within one message, preserving
  per-aggregate ordering via the session.
- Explicit `FluentValidation` for inbound payloads (currently schema + dynamic
  validation).
- Application Insights metrics for pick/unpick counts and delta distributions.

> The previous version listed the outbound Nexus / order-tracking sends as
> `TODO`/commented-out and `[CURRENTLY DISABLED]`, and described "no idempotency
> / last-write-wins" and "no distributed transactions" as gaps. All are now
> resolved by design: downstream sends go through the cached `ServiceBusSender`
> (§9), and redelivery/concurrency are handled by deterministic Id + ETag Patch
> + the §2 re-read/reapply loop (§5.6) — this is the fix for the duplicate-entry
> / doubled-quantity problem.

---

## 11. Summary

`inventory.InventoryStateChanged` processes WMS inventory state transitions on
the AKS pipeline: consumes from Kafka, relays to the `inventory-state-changed`
Service Bus queue, classifies pick / unpick / generic state-change events,
applies B2B/B2C allocation, segmentation, and B2C extension, and publishes
deltas/snapshots to `nexus-producer` and progress to `order-tracking`.

**Key business logic:** pick (B2B/B2C, with extension overage from B2B share),
unpick (DGP reversal), 3PL vs warehouse segmentation precedence, extended-state
increment/decrement guards, OMS delta only when B2C changed, ICR snapshot, and
before/after archival.

**Database updates:** ETag-guarded **Patch** (`Increment`/`Set`, ≤10 ops) on
`ItemStockInventory` and `ItemStockInventoryExtended`, with deterministic Id +
409 handling and the §2 412 re-read/reapply loop — the fix for the
duplicate-entry / doubled-quantity problem. **No last-write-wins.**

**Calculation:** `B2CExtended = B2BAVL − B2BAllocated − B2BUsedShare`;
`B2CAVL = B2COrg + B2CExtended`; `DeltaToOMS = B2CAVL_new − B2CAVL_prev`
(centralized in the shared formula helpers).

**Risks & recommendations:** concurrency conflicts expected rare once sessions
are in place; monitor dead-letter counts and Cosmos 429 rates; cache
rarely-changing lookups; alert on repeated missing-inventory skips.

---

## Appendix: Glossary

| Term | Definition |
|------|-----------|
| **B2B / B2C** | Business-to-Business (wholesale) / Business-to-Consumer (retail) domains |
| **B2BAVL / B2CAVL** | B2B / B2C available quantity (B2CAVL = original + extended) |
| **Allocated / Prepared** | Reserved for orders / picked and staged for shipment |
| **B2BUsedShare** | B2B inventory consumed to fulfil B2C demand |
| **Extended** | B2B inventory temporarily allocated to B2C demand |
| **Hallmarking** | Gold purity marking (22K, 18K, 24K) |
| **COO** | Country of Origin |
| **3PL** | Third-Party Logistics provider (e.g. CAECOM) |
| **TDC / EDC / ADC** | Fulfilment centres |
| **DGP** | Demand Generation Product (B2B product type) |
| **ICR** | Inventory Comparison Report (audit snapshot) |
| **OMNI** | Omnichannel flag |
| **SAP / OMS** | ERP master-data system / Order Management System |
| **Nexus** | Event streaming/publishing system (downstream distribution) |

---

**Document Version:** 2.0 (AKS / k8s)
**Status:** Regenerated
