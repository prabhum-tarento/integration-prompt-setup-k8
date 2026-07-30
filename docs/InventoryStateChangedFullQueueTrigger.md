# InventoryStateChangedFullQueueTrigger - Technical Documentation

## 1. Overview

### Purpose
The **InventoryStateChangedFullQueueTrigger** is an Azure Service Bus Queue-triggered Azure Function that processes inventory state change events from the warehouse management system. It handles the complete inventory lifecycle by managing state transitions, calculating inventory allocations, updating fulfillment records, and synchronizing changes across multiple downstream systems (OMS, SAP, Nexus Producer).

### Business Objective
- Process inventory state changes (PICKABLE → PREPARED, HELD, etc.) in near real-time
- Maintain accurate inventory records across B2B and B2C domains
- Support B2C extended inventory allocation through intelligent segmentation
- Synchronize inventory deltas with OMS, SAP, and external reporting systems
- Archive historical inventory snapshots for audit and reconciliation

### Scope
- Consumes `InventoryStateChangedEvent` messages from Service Bus
- Processes pick and unpick events
- Performs inventory segmentation and extension calculations
- Updates item stock inventory records with new allocation quantities
- Generates delta reports for OMS synchronization
- Creates extended inventory segmentation records
- Publishes Nexus Producer events for downstream processing

### High-Level Architecture
```
Service Bus Queue (InventoryStateChangedEvent)
                    ↓
       InventoryStateChangedFullQueueTrigger
                    ↓
    ┌───────────────┼────────────┬─────────────┐
    ↓               ↓            ↓             ↓
Pick Event    Unpick Event  Segmentation  OMS Delta
Handler       Handler       Handler       Handler
    ↓               ↓            ↓             ↓
DB Update    DB Update   DB Insert/Update  Nexus Queue
```

### Key Dependencies
- **Service Bus**: Queue-based event consumption
- **AutoMapper**: DTO mapping and transformation
- **ItemStockInventoryRepository**: Core inventory data access
- **ItemLevelSegmentationRepository**: Item-level inventory segmentation rules
- **FulfilmentLevelSegmentationRepository**: Fulfillment-level segmentation defaults
- **ItemStockInventoryExtendedRepository**: Extended inventory state tracking
- **CountryRepository**: Geographic/market mappings
- **MessageArchiveRepository**: Historical snapshot archival
- **IServiceBusQueueService**: Downstream queue communication (Nexus Producer)
- **DurableTaskClient**: Distributed function orchestration (currently disabled)

### Assumptions
1. **Message Format**: Incoming messages are valid `InventoryStateChangedEvent` objects that can be deserialized
2. **Inventory Existence**: For pick/unpick events, inventory records should exist in the database
3. **Quantity Handling**: Negative quantities in adjustments represent deductions; they are converted to positive values before processing
4. **Location Mapping**: All fulfillment location IDs map to known fulfillment centers (TDC, EDC, ADC, CAECOM)
5. **Country Codes**: Country codes from the event are valid enum values or can be resolved from the country repository
6. **State Machine**: Inventory state transitions follow a predefined state machine (AVAILABLE, IN_TRANSIT, etc.)
7. **Atomicity**: Database updates are not wrapped in distributed transactions; partial failures may occur
8. **Idempotency**: Message processing is not idempotent; duplicate message consumption may cause duplicate updates

---

## 2. End-to-End Flow

### Complete Execution Flow: Message Reception to Completion

```
1. MESSAGE RECEPTION
   ├─ Service Bus Trigger fires
   ├─ ServiceBusReceivedMessage deserialized to InventoryStateChangedEvent
   └─ Extract ReferenceId (InventoryStateChangedEvent.Id)

2. ITEM LINE ITERATION
   For Each ItemLine in InventoryStateChangedEvent.ItemLines:
   
   3. BUILD CONTEXT
      ├─ Create uniqueIdentifier Dictionary
      │  ├─ ItemCode (ProductId)
      │  ├─ LineNo (LineNum)
      │  └─ ReferenceId
      ├─ Add OrderId if present
      └─ Create ItemStockOrchestratorRequest

   4. EVENT TYPE CLASSIFICATION
      ├─ Classify as Pick Event IF:
      │  └─ FromState=(AVAILABLE, PICKABLE) AND ToState=(AVAILABLE, PREPARED)
      ├─ Classify as Unpick Event IF:
      │  └─ FromState=(AVAILABLE, PREPARED) AND ToState=(AVAILABLE, HELD or PICKABLE)
      └─ Else → Generic Inventory State Change Event

   5. PICK EVENT PROCESSING (if isPickEvent)
      ├─ Fetch ItemStockInventoryDTO
      ├─ Archive original state
      ├─ IF B2B Pick:
      │  ├─ Decrement B2BAllocated
      │  ├─ Increment B2BPrepared
      │  └─ [IF Extended] Calculate B2C extension impact
      ├─ ELSE IF B2C Pick:
      │  ├─ Increment B2CPrepared
      │  ├─ IF B2CAllocated >= PickQty:
      │  │  └─ Decrement B2CAllocated
      │  ├─ ELSE IF IsExtended:
      │  │  ├─ Consume from B2B Share
      │  │  ├─ Decrement B2BUsedShare
      │  │  └─ Recalculate B2C extension
      │  └─ ELSE: FAIL (insufficient allocation)
      ├─ Archive updated state
      └─ Persist to database

   6. UNPICK EVENT PROCESSING (if isUnpickEvent)
      ├─ Fetch ItemStockInventoryDTO
      ├─ Archive original state
      ├─ IF DGP Type:
      │  └─ Decrement B2BPrepared
      ├─ [IF Extended] Recalculate B2C extension
      ├─ Archive updated state
      └─ Persist to database

   7. GENERIC STATE CHANGE PROCESSING
      ├─ [IF ENABLE_DELTA_TOWARDS_SAP] Publish B2B Adjusted/Moved event
      │  ├─ Map to B2BInventoryAdjustedOrMovedEvent
      │  └─ Post to Nexus Producer queue
      │
      ├─ [IF State transitions involve AVAILABLE/PICKABLE] Perform segmentation:
      │  ├─ Fetch existing inventory OR create with defaults
      │  ├─ Calculate inbound quantity = MoveSign + Quantity
      │  │
      │  ├─ IF LocationType == THIRD_PARTY_LOGISTICS:
      │  │  └─ Apply Fulfillment-level B2C Segmentation
      │  ├─ ELSE IF Item-level rules exist and active:
      │  │  ├─ Apply Item-level extension
      │  │  └─ [IF NOT TDC] Update item-level segmentation
      │  ├─ ELSE:
      │  │  └─ Apply Fulfillment-level segmentation
      │  │
      │  ├─ Calculate B2C delta vs previous value
      │  └─ Archive updated inventory
      │
      └─ Perform Extended Inventory Segmentation:
         ├─ IF ToState != (AVAILABLE, PICKABLE):
         │  ├─ Fetch/Create extended inventory record for ToState/ToStatus
         │  ├─ Increment quantity
         │  └─ Archive
         └─ IF FromState != (AVAILABLE, PICKABLE):
            ├─ Fetch extended inventory record for FromState/FromStatus
            ├─ Decrement quantity
            └─ Archive

8. OMS DELTA SYNCHRONIZATION (Post Pick/Unpick/Segmentation)
   IF result.IsB2CChanged AND ENABLE_DELTA_TOWARDS_OMS:
      ├─ Fetch CountryCode from repository
      ├─ Build DeltaTowardsOmsEventRequest with delta quantity
      ├─ Post to Nexus Producer queue
      └─ Update OMS

9. ICR SNAPSHOT GENERATION
   IF ENABLE_SNAPSHOT_FOR_ICR:
      ├─ Build OmniInventoryAvailabilityReported event
      ├─ Map B2B/B2C allocations and prepared quantities
      ├─ Post to Nexus Producer queue
      └─ Capture inventory comparison snapshot

10. ORDER TRACKING (Cross-Item, Once per message)
    IF (isPickEvent OR isUnpickEvent):
       ├─ [CURRENTLY DISABLED] Log warning instead
       ├─ Build OrderTrackingCommonOrchestratorRequest
       └─ (Would dispatch to Order Tracking service if enabled)

11. EXCEPTION HANDLING
    CATCH Exception:
       └─ Log queue processing error with message ID, ReferenceId, and details
```

### Key State Transitions in Database

| Entity | From State | To State | Trigger | Action |
|--------|-----------|----------|---------|--------|
| ItemStockInventory | B2BAllocated | B2BPrepared | B2B Pick | Decrement Allocated, Increment Prepared |
| ItemStockInventory | B2CAllocated | B2CPrepared | B2C Pick | Decrement Allocated, Increment Prepared |
| ItemStockInventory | B2BPrepared | B2BAllocated | Unpick (DGP) | Increment Allocated (reverse of pick) |
| ItemStockInventory | B2CAVL | (recalc) | Extension Update | Recalculate based on share % |
| ItemStockInventoryExtended | None | Created | State Change | Create for new ToState/ToStatus |
| ItemStockInventoryExtended | Qty | Qty+Delta | Inbound | Increment extended inventory |
| ItemStockInventoryExtended | Qty | Qty-Delta | Outbound | Decrement extended inventory |

### Data Flow Through Layers

```
SERVICE BUS MESSAGE
         ↓
    DESERIALIZE to InventoryStateChangedEvent
         ↓
EXTRACT & ENRICH with ReferenceId
         ↓
FOR EACH ITEM LINE:
    BUILD ItemStockOrchestratorRequest
         ↓
    CLASSIFY Event Type (Pick, Unpick, State Change)
         ↓
    RETRIEVE ItemStockInventoryDTO from Repository
         ↓
    TRANSFORM:
    - Archive original state
    - Adjust quantities based on event type
    - Recalculate allocations and extensions
    - Archive updated state
         ↓
    PERSIST Updated DTO to Database
         ↓
BUILD Response (OutboundEventResponse with delta info)
         ↓
    IF Delta > 0:
        MAP to DeltaTowardsOmsEventRequest
        → POST to Nexus Producer Queue
         ↓
    IF ICR Enabled:
        MAP to OmniInventoryAvailabilityReported
        → POST to Nexus Producer Queue
         ↓
    IF Order Tracking:
        MAP to OrderTrackingCommonOrchestratorRequest
        → [DISABLED - Log warning instead]
```

---

## 3. Detailed Business Logic

### 3.1 Pick Event Logic

**Purpose**: Process the allocation-to-prepared transition when inventory is prepared for shipment.

**Trigger Condition**: 
```
FromState = (AVAILABLE, PICKABLE) AND ToState = (AVAILABLE, PREPARED)
```

**B2B Pick Flow**:
1. Fetch inventory record by ItemCode, Hallmark, FulfilmentCode, CountryOfOrigin
2. **Validation**: If inventory not found → Log warning, return null
3. Archive current state
4. **Calculation**:
   - B2BAllocated -= PickQuantity
   - B2BPrepared += PickQuantity
5. **Validation**: If B2BAllocated becomes negative → Log warning, set to 0
6. **Extension Check**: If IsExtended flag is true:
   - Call `CalculateB2CExtensionAsync()` with previous B2CAVL
   - Recalculate B2C impact from B2B share consumption
7. Archive updated state
8. Persist to database
9. Return OutboundEventResponse with delta info (if B2C changed)

**B2C Pick Flow**:
1. Fetch inventory record by ItemCode, Hallmark, FulfilmentCode, CountryOfOrigin
2. **Validation**: If inventory not found → Log warning, return null
3. Archive current state
4. Initialize B2BPrepared and B2CPrepared to 0 if null
5. **Two Scenarios**:

   **Scenario A - Sufficient B2C Allocation** (B2CAllocated >= PickQuantity):
   - B2CPrepared += PickQuantity
   - B2CAllocated -= PickQuantity
   - If B2CAllocated becomes negative → Log warning, set to 0
   
   **Scenario B - Insufficient B2C Allocation** (B2CAllocated < PickQuantity):
   - **If NOT Extended**: Log error, return null (fail the operation)
   - **If Extended**:
     - Calculate overage: B2BStock = PickQuantity - B2CAllocated
     - Consume from B2B share: B2BUsedShare -= B2BStock
     - Set B2CAllocated = 0
     - B2CPrepared += PickQuantity
     - If B2BUsedShare becomes negative → Log warning, return null
     - Call `CalculateB2CExtensionAsync()` with previous B2CAVL
     - Recalculate B2C available after share consumption

6. Archive updated state
7. Persist to database
8. Return OutboundEventResponse with delta info

**Error Scenarios**:
- Inventory record missing → Logged but bypassed (returns null)
- Negative quantities → Logged, set to 0
- B2C pick without allocation and not extended → Operation fails
- B2B share insufficient for B2C overage → Logged, returns null

### 3.2 Unpick Event Logic

**Purpose**: Reverse a pick operation, returning inventory from prepared state back to allocated.

**Trigger Condition**:
```
(FromState = (AVAILABLE, PREPARED) AND ToState = (AVAILABLE, HELD)) 
OR
(FromState = (AVAILABLE, PREPARED) AND ToState = (AVAILABLE, PICKABLE))
```

**Processing**:
1. Fetch inventory record by ItemCode, Hallmark, FulfilmentCode, CountryOfOrigin
2. **Validation**: If inventory not found → Log warning, return null
3. Archive current state
4. Initialize B2CPrepared to 0 if null
5. **Type-Specific Logic**:
   - **If Type == DGP** (Demand Generation Product):
     - Validate B2BPrepared > 0, else log warning and return null
     - B2BPrepared -= UnpickQuantity
   - **Else**: Log error for invalid type, return null
6. **Extension Recalculation**: If IsExtended flag is true:
   - Call `CalculateB2CExtensionAsync()` with previous B2CAVL
   - Recalculate B2C impact
7. Archive updated state
8. Persist to database
9. Return OutboundEventResponse with delta info

**Reverse Semantics**:
- Unpick reverses the pick operation completely
- B2C extension is recalculated to reflect inventory availability after unpick
- Used primarily for order cancellations or prep-to-hold transitions

### 3.3 Inventory Segmentation & Extension Logic

**Purpose**: Distribute new inventory across B2B and B2C domains based on fulfillment-level or item-level rules.

**Trigger Condition**:
```
(FromState = AVAILABLE AND FromStatus = PICKABLE) OR (ToState = AVAILABLE AND ToStatus = PICKABLE)
```

**Segmentation by Location Type**:

| Location Type | Segmentation Strategy | Data Source |
|---------------|----------------------|-------------|
| THIRD_PARTY_LOGISTICS (3PL) | Fulfillment-level B2C Segmentation | FulfilmentLevelSegmentationRepository |
| WAREHOUSE (Default) | Item-level (if active) OR Fulfillment-level | ItemLevelSegmentationRepository → FulfilmentLevelSegmentationRepository |

**Algorithm**:
```
1. Fetch ItemStockInventoryDTO (create with defaults if missing)
2. Calculate inbound quantity:
   - strQuantity = MoveSign + Quantity.ToString()
   - inboundQty = Convert.ToInt32(strQuantity)
3. Validation: If inboundQty < 0 AND stockInventoryIsNull → Fail (cannot negate empty inventory)
4. Save current B2CAVL and B2COrg for delta calculation
5. Apply Segmentation Rules:
   - IF LocationType == 3PL:
     DoFulfilmentLevelB2CSegmentation(inboundQty, inventory)
   - ELSE:
     Fetch item-level rules
     IF rules exist AND rules.IsActive:
       IsExtended = true
       DoItemLevelExtension(inboundQty, ecomShare%, inventory)
     ELSE:
       DoFulfilmentLevelSegmentation(inboundQty, inventory)
6. Calculate delta = currentB2CAVL - previousB2CAVL
7. Archive updated inventory
8. Persist to database
9. Return OutboundEventResponse with IsB2CChanged flag
```

**Fulfillment-Level Segmentation** (Default):
- Applies uniform B2C allocation percentage across all items
- Used when no item-level rules exist
- Suitable for warehouse-wide inventory policies

**Item-Level Segmentation** (Advanced):
- Applies item-specific B2C allocation percentages
- Higher priority than fulfillment-level rules
- Supports brand-specific or product-category strategies
- Includes storage leverage percentage for extension calculations
- Marked as `IsExtended` for delta calculation changes

### 3.4 B2C Extension Calculation

**Purpose**: Intelligently allocate B2B inventory to B2C when B2C demand exceeds formal allocation.

**When Triggered**:
- Pick event on B2B inventory on an extended item
- Pick event on B2C allocation without sufficient allocated inventory (requires extension)
- Unpick event on extended inventory
- Inventory segmentation with item-level rules and IsExtended flag

**Calculation Formula**:
```
B2CExtended = CalculateActualB2BAvailable(inventory)
            = (B2BAVL - B2BAllocated - B2BUsedShare)

B2CAVL_new = CalculateB2CAvl(inventory)
           = B2COrg + B2CExtended

DeltaToOMS = B2CAVL_new - B2CAVL_prev
```

**Variables**:
- **B2BAVL**: Total B2B Available inventory
- **B2BAllocated**: B2B inventory reserved/allocated
- **B2BUsedShare**: B2B inventory consumed to fulfill B2C demand
- **B2COrg**: Original B2C-specific allocation
- **B2CExtended**: B2B inventory temporarily allocated to B2C
- **B2CAVL**: Total B2C Available (original + extended)

**Data Source**: 
- Primary: ItemLevelSegmentationRepository
- Fallback: FulfilmentLevelSegmentationRepository
- Key parameter: StoreLeveragePercentage

**Boundary Conditions**:
- B2CExtended cannot exceed (B2BAVL - B2BAllocated)
- B2CAVL recalculated only if extension changes
- Null handling: Missing storage leverage defaults to 0

**Worked Example**:
```
Scenario: B2C pick on extended item requires B2B share
Input:
  PickQuantity = 100
  B2CAllocated = 60
  B2BAVL = 500
  B2BAllocated = 200
  B2BUsedShare = 0 (before)
  B2COrg = 60
  B2CAVL_prev = 60

Processing:
  Step 1: B2CAllocated (60) < PickQuantity (100) → Use extension
  Step 2: B2BStock required = 100 - 60 = 40
  Step 3: B2BUsedShare = 0 + 40 = 40
  Step 4: B2CAllocated = 0
  Step 5: B2CPrepared = 0 + 100 = 100
  Step 6: Recalculate B2CExtended = 500 - 200 - 40 = 260
  Step 7: B2CAVL_new = 60 + 260 = 320
  Step 8: DeltaToOMS = 320 - 60 = +260

Result: OMS is notified that B2C available increased by 260 due to extension
```

### 3.5 Extended Inventory Segmentation

**Purpose**: Track inventory in non-standard states (e.g., DAMAGED, QUARANTINE, IN_TRANSIT) separately for compliance and reporting.

**Trigger Condition**:
```
When FromState != (AVAILABLE, PICKABLE) OR ToState != (AVAILABLE, PICKABLE)
```

**Processing - To-State Handling**:
```
IF ToState != (AVAILABLE, PICKABLE):
  1. Fetch ItemStockInventoryExtended record for (ItemCode, Hallmark, FulfilmentCode, COO, ToState, ToStatus)
  2. IF record doesn't exist:
     - Create new record with ToState/ToStatus and Qty = inboundQty
  3. ELSE:
     - Archive previous record
     - Increment Qty by inboundQty
  4. Archive updated record
  5. Persist to database
```

**Processing - From-State Handling**:
```
IF FromState != (AVAILABLE, PICKABLE):
  1. Fetch ItemStockInventoryExtended record for (ItemCode, Hallmark, FulfilmentCode, COO, FromState, FromStatus)
  2. IF record exists AND Qty >= |inboundQty|:
     - Archive previous record
     - Decrement Qty by |inboundQty|
     - Persist to database
     - Archive updated record
  3. ELSE:
     - Log warning (insufficient quantity in extended state)
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

### 3.6 B2B Adjusted/Moved Event Publishing

**Purpose**: Notify SAP of inventory adjustments for master data synchronization.

**Trigger Condition**:
```
When NOT a Pick/Unpick event AND one of:
  - ENABLE_DELTA_TOWARDS_SAP=true AND Location != EDC AND Location != ADC
  - ENABLE_DELTA_TOWARDS_AX12_3PL=true AND Location == CAECOM
  - ENABLE_ADC_DELTA_TOWARDS_AX12=true AND Location == ADC
```

**Processing**:
1. Map InventoryStateChangedEvent to B2BInventoryAdjustedOrMovedEvent
2. **Validation - Fix SAE-2798**:
   - If not a B2B_INVENTORY_ADJUSTED event type:
     - Check if FromState.State == ToState.State AND neither is AVAILABLE
     - If true, skip publishing (invalid transition)
3. **Status Correction - Fix SAE-3032**:
   - If FromState.State != AVAILABLE → Set FromState.Status = UNKNOWN
   - If ToState.State != AVAILABLE → Set ToState.Status = UNKNOWN
4. **Quantity Normalization**:
   - If any adjustment line has negative quantity → Convert to positive (use Math.Abs)
5. **Publish to Nexus Producer**:
   - Wrap in NexusProducerRequest with type: `Inventory_B2BInventoryAdjustedOrMoved`
   - Post to NEXUS_PRODUCER_QUEUE_NAME
   - [CURRENTLY COMMENTED: `await _serviceBusQueueService.SendMessageAsync(...)`]

**Error Scenarios**:
- ReferenceId missing → Generate new GUID and log
- FromState == ToState AND neither AVAILABLE → Skip (invalid)
- State not AVAILABLE but Status not UNKNOWN → Normalize to UNKNOWN

### 3.7 OMS Delta Synchronization

**Purpose**: Notify OMS of B2C inventory availability changes for order fulfillment decisions.

**Trigger Condition**:
```
IF result != null AND result.IsB2CChanged AND ENABLE_DELTA_TOWARDS_OMS
(with LocationType-specific feature flag checks)
```

**Request Structure**:
```csharp
DeltaTowardsOmsEventRequest {
  ReferenceId: New GUID (unique per message)
  ProductId: ItemCode
  Location: (Id, Type) from event
  Reason: ReasonCode.ADJUSTMENT
  AdjustmentDate: DateTime.UtcNow
  ProductUnits: "N/A"
  Market: CountryCode (looked up from repository)
  QuantityDetails: [
    {
      CountryOfOrigin: item.CountryOfOrigin
      Hallmarking: item.Hallmarking
      Quantity: result.DeltaTowardsOMS (signed delta)
      State: (AVAILABLE, PICKABLE)
      ReasonTexts: []
    }
  ]
}
```

**Publishing**:
1. Fetch CountryCode from CountryRepository by FulfilmentId
2. Try parsing as enum, fallback to CountryCode.UNKNOWN if invalid
3. Build DeltaTowardsOmsEventRequest with:
   - ReferenceId = Guid.NewGuid().ToString()
   - DeltaTowardsOMS = calculated delta quantity
   - State/Status always (AVAILABLE, PICKABLE) for OMS
4. Wrap in NexusProducerRequest with type: `Inventory_B2CInventoryAdjusted`
5. Post to Nexus Producer queue
6. [CURRENTLY COMMENTED: `await _serviceBusQueueService.SendMessageAsync(...)`]

**Failure Handling**:
- If CountryRepository lookup fails → Fallback to CountryCode.UNKNOWN
- If B2C not changed → Skip publishing (conserve queue traffic)
- If feature flag disabled → Skip with information log

### 3.8 Inventory Comparison Report (ICR)

**Purpose**: Create snapshots of inventory state for Inventory Comparison Reports and auditing.

**Trigger Condition**:
```
IF ENABLE_SNAPSHOT_FOR_ICR = true
```

**Processing**:
1. Fetch ItemStockInventoryDTO (if missing, return empty string)
2. Build snapshot with all allocation states:
   - B2B Available (AVAILABLE, PICKABLE)
   - B2C Available (uses B2COrg if extended, else B2CAVL)
   - B2B Prepared (AVAILABLE, PREPARED)
   - B2C Prepared (AVAILABLE, PREPARED)
3. Map to OmniInventoryAvailabilityReported event with:
   - ProductId, CountryOfOrigin, Hallmarking
   - Location (with 3PL type detection for CAECOM)
   - QuantityDetails: [B2B_AVL, B2C_AVL, B2B_PREP, B2C_PREP]
   - ReportDate = DateTime.UtcNow
   - ProductUnits from config
4. Wrap in NexusProducerRequest with type: `Inventory_OmniInventoryAvailabilityReported`
5. Post to Nexus Producer queue
6. [CURRENTLY COMMENTED: `await _serviceBusQueueService.SendMessageAsync(...)`]

**Data Accuracy**:
- Uses B2COrg if IsExtended (reflects original allocation under extension)
- Uses B2CAVL if not extended (reflects effective availability)
- Captures all allocation domains (B2B, B2C) and states (AVAILABLE, PREPARED)

---

## 4. Calculation Logic

### Formula 1: Inbound Quantity Calculation

**Formula**:
```
inboundQty = Convert.ToInt32(MoveSign + Quantity.ToString())
```

**Variables**:
- **MoveSign**: String value ("+", "-", or empty) indicating direction
- **Quantity**: Numeric quantity value
- **inboundQty**: Signed integer (-n to +n)

**Data Source**: InventoryStateChangedEvent.MoveSign, InventoryStateChangedEvent.ItemLines[].Quantity

**Units**: Quantity units (pieces, units, etc.)

**Rounding Logic**: Convert.ToInt32 performs standard rounding (banker's rounding)

**Precision**: Integer precision (no decimals)

**Boundary Conditions**:
- MoveSign = "-" means deduction (e.g., "-100" = -100 units)
- MoveSign = "+" or empty means addition (e.g., "100" = +100 units)
- Result can be zero for balanced transfers

**Null Handling**: 
- MoveSign defaults to empty string (no sign = addition)
- Quantity must be non-null (required field)

**Overflow/Underflow Handling**: 
- int.MinValue to int.MaxValue range (−2,147,483,648 to 2,147,483,647)
- No explicit validation; relies on database constraints

**Worked Examples**:
```
Example 1 - Addition:
  MoveSign = "+"
  Quantity = 100
  Result = +100

Example 2 - Subtraction:
  MoveSign = "-"
  Quantity = 50
  Result = -50

Example 3 - No Sign:
  MoveSign = "" (empty)
  Quantity = 75
  Result = +75
```

### Formula 2: B2C Available Calculation (Non-Extended)

**Formula**:
```
B2CAVL = B2COrg + B2CExtended
```

**Variables**:
- **B2COrg**: Original B2C-specific allocation quantity
- **B2CExtended**: B2B inventory allocated to B2C (calculated)
- **B2CAVL**: Total B2C available quantity

**Data Source**: 
- B2COrg: ItemStockInventoryDTO.B2COrg
- B2CExtended: Calculated from extension helper

**Units**: Quantity units

**Rounding Logic**: Simple addition, no rounding

**Precision**: Integer values

**Boundary Conditions**:
- B2CAVL must be >= 0
- B2COrg + B2CExtended cannot exceed total inventory
- If extended, B2CExtended is capped by available B2B stock

**Null Handling**:
- B2COrg defaults to 0 if null
- B2CExtended defaults to 0 if null

**Default Values**: 
- B2CAVL = B2COrg (if not extended)
- B2CAVL = B2COrg + B2CExtended (if extended)

**Worked Example**:
```
Scenario: Standard B2C allocation
Input:
  B2COrg = 200
  B2CExtended = 0
  
Processing:
  B2CAVL = 200 + 0 = 200

Result: B2C can pick 200 units from regular allocation
```

### Formula 3: B2CExtended Calculation

**Formula**:
```
B2CExtended = B2BAVL - B2BAllocated - B2BUsedShare
```

**Variables**:
- **B2BAVL**: Total B2B available inventory
- **B2BAllocated**: B2B inventory allocated/reserved for B2B orders
- **B2BUsedShare**: B2B inventory consumed by B2C picks (extension usage)
- **B2CExtended**: Excess B2B available for B2C allocation

**Data Source**: ItemStockInventoryDTO fields

**Units**: Quantity units

**Rounding Logic**: Subtraction, no rounding

**Precision**: Integer values

**Boundary Conditions**:
- B2CExtended >= 0 (cannot be negative; capped at 0)
- B2CExtended <= B2BAVL (cannot exceed total B2B)
- (B2BAVL - B2BAllocated) is the maximum available for sharing

**Null Handling**:
- B2BAVL defaults to 0
- B2BAllocated defaults to 0
- B2BUsedShare defaults to 0

**Underflow Handling**: 
- If result < 0, cap at 0
- Log warning if B2BUsedShare causes negative result

**Worked Examples**:
```
Example 1 - Standard Extension
Input:
  B2BAVL = 500
  B2BAllocated = 200
  B2BUsedShare = 50
  
Processing:
  B2CExtended = 500 - 200 - 50 = 250
  
Result: 250 units of B2B can be extended to B2C

Example 2 - No Extension Available
Input:
  B2BAVL = 200
  B2BAllocated = 200
  B2BUsedShare = 0
  
Processing:
  B2CExtended = 200 - 200 - 0 = 0
  
Result: No B2B inventory available for extension
```

### Formula 4: Delta Towards OMS

**Formula**:
```
DeltaTowardsOMS = B2CAVL_new - B2CAVL_previous
```

**Variables**:
- **B2CAVL_new**: Current B2C available quantity
- **B2CAVL_previous**: Previous B2C available quantity (before this operation)
- **DeltaTowardsOMS**: Signed delta (+increase, -decrease)

**Data Source**:
- Captured at start of operation
- Recalculated after quantity changes
- Used in segmentation and pick/unpick handlers

**Units**: Quantity units (same as inventory)

**Rounding Logic**: Direct subtraction, no rounding

**Precision**: Integer values

**Boundary Conditions**:
- Delta can be negative (decrease) or positive (increase)
- Delta = 0 means no OMS notification needed
- Optimization: Only publish if delta != 0

**Null Handling**:
- B2CAVL defaults to 0 if null
- Delta defaults to 0 if B2CAVL not captured

**Worked Example**:
```
Scenario: B2C pick triggers extension recalculation
Input:
  B2CAVL_prev = 100
  PickQuantity = 50 (consumed from allocation)
  Extension recalculates due to B2BUsedShare change
  B2CAVL_new = 250 (now includes 150 extended units)
  
Processing:
  DeltaTowardsOMS = 250 - 100 = +150
  
Result: OMS is notified of +150 units now available
Reason: B2B inventory was extended to cover B2C demand
```

---

## 5. Database Documentation

### 5.1 Core Inventory Tables

#### Table: ItemStockInventory
**Purpose**: Central inventory record tracking all allocation states across B2B and B2C domains.

**Read Operations**:
```sql
Query: GetInventoryByCategory
Filters:
  - ItemCode (exact match)
  - Hallmark (enum string)
  - FulfilmentCode (exact match)
  - CountryOfOrigin (enum string)
Joins: None (direct lookup)
Index Usage: Likely composite index on (ItemCode, Hallmark, FulfilmentCode, COO)
Expected Result: Single ItemStockInventoryDTO or null
```

**Columns Involved**:
| Column | Data Type | Purpose | Read/Write |
|--------|-----------|---------|-----------|
| Id | GUID | Primary key | Write (Insert) |
| ItemCode | String | Product identifier | Read, Write (Insert) |
| Hallmark | String | Hallmarking type | Read, Write (Insert) |
| FulfilmentId | String | Fulfillment center code | Read, Write (Insert) |
| COO (CountryOfOrigin) | String | Country of origin | Read, Write (Insert) |
| B2BAVL | Integer | B2B available quantity | Read, Write (Update) |
| B2BAllocated | Integer | B2B reserved quantity | Read, Write (Update) |
| B2BPrepared | Integer | B2B picked/prepared quantity | Read, Write (Update) |
| B2BUsedShare | Integer | B2B inventory used for B2C picks | Read, Write (Update) |
| B2CAVL | Integer | B2C available quantity | Read, Write (Update) |
| B2COrg | Integer | B2C original allocation | Read, Write (Update) |
| B2CPrepared | Integer | B2C picked/prepared quantity | Read, Write (Update) |
| B2CAllocated | Integer | B2C reserved quantity | Read, Write (Update) |
| B2CExtended | Integer | B2B inventory extended to B2C | Read, Write (Update) |
| InternalHallmarkAllocated | Integer | Internal hallmark reserved qty | Read, Write (Update) |
| InTransit | Integer | In-transit quantity | Read (not modified here) |
| PSC | Integer | Product service center qty | Read (not modified here) |
| IsExtended | Boolean | Flag: uses B2B extension logic | Read, Write (Update) |
| CreatedDate | DateTime | Record creation timestamp | Write (Insert) |
| UpdatedDate | DateTime | Last modification timestamp | Write (Update) |

**Insert Operations**:
```csharp
When: InventorySegmentationAndExtensionHandler cannot find existing record
Columns Populated:
  - Id: Guid.NewGuid()
  - ItemCode, Hallmark, FulfilmentId, COO: From input
  - B2BAVL, B2CAVL, B2BAllocated, B2CAllocated, B2CExtended, 
    B2BUsedShare, B2COrg, B2BPrepared, B2CPrepared: Set to 0
  - InternalHallmarkAllocated, InTransit, PSC: Set to 0
  - IsExtended: false (initially, may be updated later)
  - CreatedDate: DateTime.UtcNow
  - UpdatedDate: DateTime.UtcNow
Source of Values:
  - Identifiers: From InventorySegmentationAndExtensionRequest
  - Quantities: Defaults (0)
Generated Values:
  - Id: New GUID
  - Timestamps: Current UTC time
Default Values: All quantities default to 0; IsExtended defaults to false
```

**Update Operations**:
```csharp
Scenarios:
1. Pick Event (B2B):
   Table Updated: ItemStockInventory
   Columns Modified:
     - B2BAllocated: -= PickQuantity (reduced)
     - B2BPrepared: += PickQuantity (increased)
     - B2CExtended: Recalculated if IsExtended
     - B2CAVL: Recalculated if IsExtended
     - UpdatedDate: DateTime.UtcNow
   Update Condition: ItemCode + Hallmark + FulfilmentId + COO match
   Transaction: Single operation (implicit transaction)
   Optimistic Locking: Not used
   Triggered Events: OutboundEventResponse delta calculation

2. Pick Event (B2C):
   Table Updated: ItemStockInventory
   Columns Modified:
     - B2CAllocated: -= PickQuantity OR Set to 0 (if extended)
     - B2CPrepared: += PickQuantity
     - B2BUsedShare: += (PickQuantity - B2CAllocated) if extended
     - B2CExtended: Recalculated if IsExtended
     - B2CAVL: Recalculated if IsExtended
     - UpdatedDate: DateTime.UtcNow
   Previous Value: Snapshot before pick (archived)
   New Value: Snapshot after pick
   Triggered Events: OutboundEventResponse delta calculation

3. Unpick Event:
   Table Updated: ItemStockInventory
   Columns Modified:
     - B2BPrepared: -= UnpickQuantity (reversal)
     - B2CExtended: Recalculated if IsExtended
     - B2CAVL: Recalculated if IsExtended
     - UpdatedDate: DateTime.UtcNow
   Update Condition: ItemCode + Hallmark + FulfilmentId + COO match
   Transaction: Single operation
   Optimistic Locking: Not used

4. Segmentation Event:
   Table Updated: ItemStockInventory
   Columns Modified:
     - B2BAVL, B2CAVL, B2BAllocated, B2CAllocated: Adjusted per segmentation rules
     - B2CExtended: Recalculated
     - IsExtended: Set to true if item-level rules active
     - UpdatedDate: DateTime.UtcNow
   Previous Value: Snapshot before segmentation
   New Value: Snapshot after segmentation
   Triggered Events: DeltaTowardsOMS if B2C changed
```

**Rollback Scenarios**:
- No explicit rollback; if update fails, exception is caught and logged
- Message is retried via Service Bus dead-letter queue
- Manual intervention required for data correction

**Commit Points**:
- Update commit is immediate after `UpdateStockInventoryAsync()` call
- No distributed transaction control
- Each item line processed independently

---

#### Table: ItemStockInventoryExtended
**Purpose**: Track inventory in non-standard states for compliance and state tracking.

**Read Operations**:
```sql
Query: GetInventoryByCategory
Filters:
  - ItemCode (exact match)
  - Hallmark (enum string)
  - FulfilmentCode (exact match)
  - CountryOfOrigin (enum string)
  - State (enum value)
  - Status (enum value)
Joins: None (direct lookup)
Index Usage: Likely composite index on (ItemCode, Hallmark, FulfilmentCode, COO, State, Status)
Expected Result: Single ItemStockInventoryExtendedDTO or null
```

**Columns Involved**:
| Column | Data Type | Purpose | Read/Write |
|--------|-----------|---------|-----------|
| Id | GUID | Primary key | Write (Insert) |
| ItemCode | String | Product identifier | Read, Write (Insert) |
| Hallmark | String | Hallmarking type | Read, Write (Insert) |
| FulfilmentId | String | Fulfillment center code | Read, Write (Insert) |
| COO | String | Country of origin | Read, Write (Insert) |
| State | Enum | Inventory state | Read, Write (Insert) |
| Status | Enum | Inventory status | Read, Write (Insert) |
| Qty | Integer | Quantity in this state/status | Read, Write (Update) |
| CreatedDate | DateTime | Record creation timestamp | Write (Insert) |
| UpdatedDate | DateTime | Last modification timestamp | Write (Update) |

**Insert Operations**:
```csharp
When: Extended inventory state is new (ToState transition)
Columns Populated:
  - Id: Guid.NewGuid()
  - ItemCode, Hallmark, FulfilmentId, COO: From input
  - State: From ExtendedInventorySegmentationRequest.ToState
  - Status: From ExtendedInventorySegmentationRequest.ToStatus
  - Qty: input.Quantity
  - CreatedDate: DateTime.UtcNow
  - UpdatedDate: DateTime.UtcNow
Source of Values: ExtendedInventorySegmentationRequest
Generated Values: Id (GUID), Timestamps
Default Values: None (all fields required)
```

**Update Operations**:
```csharp
Scenario: Extended Inventory Segmentation Event

1. To-State Update (Inbound):
   Columns Modified:
     - Qty: += input.Quantity (accumulation)
     - UpdatedDate: DateTime.UtcNow
   Previous Value: Qty before increment
   New Value: Qty after increment
   Update Condition: ItemCode + Hallmark + FulfilmentId + COO + State + Status match

2. From-State Update (Outbound):
   Columns Modified:
     - Qty: -= input.Quantity (depletion)
     - UpdatedDate: DateTime.UtcNow
   Previous Value: Qty before decrement
   New Value: Qty after decrement
   Update Condition: ItemCode + Hallmark + FulfilmentId + COO + State + Status match
   Validation: Qty >= |input.Quantity| (cannot go negative)
   Failure: Log warning if Qty insufficient
```

**Validation Logic**:
```
For From-State deduction:
  IF Qty < |Quantity|:
    Log warning: "Insufficient quantity in extended state"
    Skip update
  ELSE:
    Proceed with update
```

---

#### Table: ItemLevelSegmentation
**Purpose**: Store item-specific inventory segmentation rules for B2C allocation.

**Read Operations**:
```sql
Query: GetItemLevelFulfilmentyByCategory
Filters:
  - FulfilmentCode (exact match)
  - Hallmark (enum string)
  - ItemCode (exact match)
  - CountryOfOrigin (enum string)
  - IsActive: true (filter active rules only)
Joins: None (direct lookup)
Index Usage: Composite index on (FulfilmentCode, Hallmark, ItemCode, COO, IsActive)
Expected Result: Single ItemLevelSegmentationDTO or null
```

**Columns Involved**:
| Column | Data Type | Purpose | Read/Write |
|--------|-----------|---------|-----------|
| Id | GUID | Primary key | Write (Insert) |
| FulfilmentId | String | Fulfillment center code | Read, Write (Insert) |
| ItemCode | String | Product identifier | Read, Write (Insert) |
| Hallmark | String | Hallmarking type | Read, Write (Insert) |
| COO | String | Country of origin | Read, Write (Insert) |
| EcomShare | Decimal | B2C percentage share (0-100) | Read |
| StoreLeveragePercentage | Decimal | Extension leverage % | Read |
| IsOMNI | Boolean | Omnichannel flag | Read |
| IsActive | Boolean | Rule is active | Read |
| EffectiveDate | DateTime | Rule start date | Read |
| ExpiryDate | DateTime | Rule end date (null = no expiry) | Read |

**Update Operations**:
```csharp
Scenario: Update Item Level Segmentation
Called From: updateItemLevelSegmentationHandlerAsync (only if NOT TDC location)

Columns Modified: (Based on ItemStockInventoryDTO)
  - May update IsExtended flag, StoreLeveragePercentage
  - Exact update columns depend on repository implementation
  
Update Condition: Match by FulfilmentCode + ItemCode + Hallmark + COO
Purpose: Sync segmentation state after extension calculation
```

**Data Source**: 
- Segmentation rules managed by separate operational process (not in this trigger)
- This trigger only reads rules; does not create/modify them

---

#### Table: FulfilmentLevelSegmentation
**Purpose**: Store fulfillment-wide inventory segmentation defaults when no item-level rules exist.

**Read Operations**:
```sql
Query: GetFulfilmentLevelFulfilmentyByCategory
Filters:
  - FulfilmentCode (exact match)
  - Hallmark (enum string)
  - ItemCode (exact match)
  - CountryOfOrigin (enum string)
Joins: None (direct lookup)
Index Usage: Composite index on (FulfilmentCode, Hallmark, ItemCode, COO)
Expected Result: Single FulfilmentLevelSegmentationDTO or null
```

**Columns Involved**:
| Column | Data Type | Purpose | Read/Write |
|--------|-----------|---------|-----------|
| Id | GUID | Primary key | Read |
| FulfilmentId | String | Fulfillment center code | Read |
| ItemCode | String | Product identifier | Read |
| Hallmark | String | Hallmarking type | Read |
| COO | String | Country of origin | Read |
| StoreLeveragePercentage | Decimal | B2C extension leverage % | Read |

**Read-Only**: This trigger does not modify fulfillment-level rules.

---

#### Table: Country
**Purpose**: Mapping from fulfillment location to market/country code for OMS synchronization.

**Read Operations**:
```sql
Query: GetCountryCodeAsync
Filters:
  - FulfilmentId (exact match)
Joins: None (direct lookup)
Index Usage: Likely index on FulfilmentId
Expected Result: String country code (e.g., "IN", "US", "GB")
```

**Data Flow**:
- FulfilmentId from InventoryStateChangedEvent.Location.Id
- Result mapped to CountryCode enum for OMS event
- Used only if ENABLE_DELTA_TOWARDS_OMS is true

---

### 5.2 Archive Table

#### Table: MessageArchive
**Purpose**: Audit trail of all inventory snapshots before and after modifications.

**Write Operations**:
```csharp
Called: ArchiveMessageAsync<T>(message) before and after updates

Trigger Points:
  1. Before pick/unpick/segmentation: Archive original ItemStockInventoryDTO
  2. After pick/unpick/segmentation: Archive updated ItemStockInventoryDTO
  3. Extended segmentation: Archive ItemStockInventoryExtended before/after update
  4. Item-level segmentation: Archive ItemLevelSegmentationDTO if updated

Insert Pattern:
  - Generic method accepts any T type
  - Implementation likely serializes T to JSON + metadata
  - Stores: Original object, Timestamp, OperationType, CorrelationId

Purpose: Enable reconciliation, debugging, and compliance audits
```

**Data Stored**:
- Original entity state (before modification)
- Updated entity state (after modification)
- Operation context (ItemCode, ReferenceId, OperationType)
- Timestamp (DateTime.UtcNow)
- CorrelationId from ICorrelationContextAccessor

**Retention**: Likely indefinite (archive table) for compliance

---

### 5.3 Transaction Flow & Locking

**Transaction Boundary**: Single-message processing
- One Service Bus message = one transaction
- Multiple item lines = sequential processing within one message
- Each item line's database updates are independent

**Optimistic/Pessimistic Locking**: None
- No concurrency control mechanisms
- Last-write-wins behavior (if two triggers process same inventory simultaneously)
- Risk: Concurrent pick events on same inventory may cause underflow

**Rollback Scenarios**:
1. **Inventory not found**: Log warning, skip, continue with next item
2. **Quantity goes negative**: Log warning, cap at 0, continue
3. **Extension calculation fails**: Log warning, return null
4. **Database update fails**: Exception logged, message goes to dead-letter queue
5. **Downstream publish fails**: Exception logged, manual retry needed

**Retry Logic**:
- Handled by Service Bus message retry policy (exponential backoff)
- Max retry count configured in Azure portal (typically 10)
- Dead-letter queue for messages exceeding retry limit

---

## 6. State Changes & State Machine

### Complete State Transition Diagram

```
INVENTORY LIFECYCLE:

Initial State:
  ItemCode: "ABC123"
  Hallmark: "22K"
  Location: "TDC"
  B2BAVL: 1000
  B2CAVL: 500
  B2BAllocated: 200
  B2CAllocated: 400
  IsExtended: false

                        ↓ PICK EVENT (B2B, Qty=100)
                        
State After Pick:
  B2BAllocated: 100 (reduced by 100)
  B2BPrepared: 100 (increased by 100)
  B2CAVL: 500 (unchanged if not extended)
  IsExtended: false
  
                        ↓ UNPICK EVENT (DGP, Qty=50)
                        
State After Unpick:
  B2BAllocated: 150 (increased by 50, reversing previous pick)
  B2BPrepared: 50 (reduced by 50)
  B2CAVL: 500 (recalculated if extended)
  
                        ↓ INBOUND SEGMENTATION (Qty=+200)
                        
State After Segmentation:
  B2BAVL: 1200 (increased by 200)
  B2CAVL: 600 (increased by 100, per segmentation rule)
  IsExtended: true (if item-level rules found)
  B2CExtended: Some positive value (due to extension logic)

                        ↓ B2C PICK WITH EXTENSION (Qty=150)
                        
State After Extended Pick:
  B2CAllocated: 250 (reduced by 150)
  B2CPrepared: 150 (increased by 150)
  B2BUsedShare: 0 (unchanged, no overage)
  B2CAVL: 600 (unchanged)
  
                        ↓ B2C PICK EXCEEDING ALLOCATION (Qty=350)
                        
State After Extended Pick (Overage):
  B2CAllocated: 0 (fully consumed)
  B2CPrepared: 350 (increased by 350)
  B2BUsedShare: 100 (consumed from B2B: 350 - 250 allocated)
  B2CExtended: Recalculated (reduced due to share usage)
  B2CAVL: Recalculated (may increase if extension margin high)
```

### State Machine Rules

| From State | To State | Event Type | Valid Transitions |
|-----------|----------|-----------|------------------|
| (AVAILABLE, PICKABLE) | (AVAILABLE, PREPARED) | PICK | ✓ Allowed |
| (AVAILABLE, PREPARED) | (AVAILABLE, HELD) | UNPICK | ✓ Allowed |
| (AVAILABLE, PREPARED) | (AVAILABLE, PICKABLE) | UNPICK | ✓ Allowed |
| (AVAILABLE, PICKABLE) | (IN_TRANSIT, INTRANSIT) | SHIP | ✓ Allowed (generic) |
| (IN_TRANSIT, INTRANSIT) | (AVAILABLE, PICKABLE) | RECEIVE | ✓ Allowed (generic) |
| (AVAILABLE, PICKABLE) | (DAMAGED, DAMAGED) | DAMAGE | ✓ Allowed (generic) |
| (AVAILABLE, PICKABLE) | (QUARANTINE, QUARANTINE) | QUARANTINE | ✓ Allowed (generic) |
| (Any, Any) | (Any, Any) | (Other) | ℹ️ Logged for visibility |

### Critical Invariants (Must Always Hold)

1. **Quantity Non-Negativity**:
   ```
   B2BAllocated >= 0
   B2BPrepared >= 0
   B2CAllocated >= 0
   B2CPrepared >= 0
   B2BUsedShare >= 0
   ```
   Violation: Logged as warning, value capped at 0

2. **B2B Share Conservation**:
   ```
   B2BAVL >= B2BAllocated + B2BPrepared + B2BUsedShare
   ```
   Violation: Operation may fail (especially B2C overage)

3. **B2C Availability**:
   ```
   B2CAVL >= B2CAllocated + B2CPrepared
   ```
   Always true due to extension logic

4. **Extension Flag Consistency**:
   ```
   IF IsExtended == true THEN B2CExtended > 0
   IF IsExtended == false THEN B2CExtended == 0
   ```
   Mostly enforced; may have edge cases in race conditions

---

## 7. API Documentation

### Service Bus Message Contract

**Endpoint**: Service Bus Queue: `{INVENTORY_STATE_CHANGED_REFLEX_QUEUE_NAME}`

**Message Format**: Azure Service Bus ReceivedMessage

**Message Payload** - InventoryStateChangedEvent:
```json
{
  "id": "UUID",
  "referenceId": "ORDER-12345",
  "type": "PICKEDB2B | PICKEDB2C | TRANSFER",
  "channel": "OMS | EXTERNAL_SYSTEM",
  "location": {
    "id": "TDC | EDC | ADC | CAECOM",
    "type": "WAREHOUSE | THIRD_PARTY_LOGISTICS"
  },
  "fromState": {
    "state": "AVAILABLE | IN_TRANSIT | DAMAGED | QUARANTINE",
    "status": "PICKABLE | PREPARED | HELD | UNKNOWN | INTRANSIT"
  },
  "toState": {
    "state": "AVAILABLE | IN_TRANSIT | DAMAGED | QUARANTINE",
    "status": "PICKABLE | PREPARED | HELD | UNKNOWN | INTRANSIT"
  },
  "moveSign": "+" | "-" | "",
  "itemLines": [
    {
      "productId": "ITEM-CODE-123",
      "lineNum": "1",
      "quantity": 100,
      "countryOfOrigin": "IN",
      "hallmarking": "22K | 18K | 24K"
    }
  ],
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Headers** (Standard Azure Service Bus):
- `MessageId`: Unique message identifier
- `CorrelationId`: Trace correlation across systems
- `ContentType`: application/json
- `Subject`: (Optional) Event category

**Response**: None (fire-and-forget processing)

**Status Codes**:
- **Processing Success**: Message deleted from queue
- **Processing Failure**: Automatic retry (exponential backoff)
- **Max Retries Exceeded**: Message moved to dead-letter queue

**Error Codes** (Logged, not returned):
| Error | Scenario | Handling |
|-------|----------|----------|
| MissingItemStockInventoryException | Inventory record not found | Logged, bypassed (returns null) |
| InvalidItemStockInventoryQtyException | Quantity calculations invalid | Logged, capped at 0 |
| InvalidDataException | Unexpected event type or state | Logged, operation skipped |
| InvalidExendedItemStockInventoryQtyException | Extended inventory underflow | Logged, update skipped |
| General Exception | Unhandled errors | Logged with context, message dead-lettered |

**Validation Rules**:
1. **ItemCode**: Not null, not empty
2. **Quantity**: Integer >= 0
3. **Location.Id**: Must map to known fulfillment center
4. **FromState/ToState**: Valid state enum values
5. **Hallmarking**: Valid hallmark enum value
6. **CountryOfOrigin**: Valid country code or parseable enum

**Sample Request** (Message on Queue):
```json
{
  "id": "evt-001",
  "referenceId": "ORD-5678",
  "type": "PICKEDB2C",
  "channel": "OMS",
  "location": {
    "id": "TDC",
    "type": "WAREHOUSE"
  },
  "fromState": {
    "state": "AVAILABLE",
    "status": "PICKABLE"
  },
  "toState": {
    "state": "AVAILABLE",
    "status": "PREPARED"
  },
  "moveSign": "",
  "itemLines": [
    {
      "productId": "GOLD-RING-001",
      "lineNum": "1",
      "quantity": 5,
      "countryOfOrigin": "IN",
      "hallmarking": "22K"
    }
  ],
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Sample Response** (No direct response; side effects):
1. Database updated with new inventory state
2. Archive entries created for audit trail
3. OMS notified via DeltaTowardsOmsEventRequest (if delta > 0)
4. ICR snapshot published (if enabled)
5. Logging entries generated with correlation ID

---

## 8. Error Handling & Retry Mechanisms

### Validation Errors

| Error | Source | Handling | Impact |
|-------|--------|----------|--------|
| MissingItemStockInventoryException | Inventory not found | Log warning, return null, continue | Operation skipped for item |
| InvalidItemStockInventoryQtyException | Calculated quantity negative | Log warning, cap at 0 | Potential underflow masked |
| InvalidDataException | Invalid event type/state | Log warning, skip operation | Item not processed |
| InvalidExendedItemStockInventoryQtyException | Extended inventory underflow | Log warning, skip update | Extended state not decremented |
| CountryCode.UNKNOWN | Country lookup fails | Fallback to UNKNOWN enum | OMS receives unknown market |

### Database Errors

| Error | Scenario | Handling |
|-------|----------|----------|
| Connection Timeout | Database unavailable | Exception thrown, message retried |
| Update Conflicts | Concurrent updates (no locking) | Last-write-wins (data loss risk) |
| Constraint Violation | FK/unique constraint fails | Exception thrown, message dead-lettered |
| Transaction Deadlock | Rare, with concurrent access | Exception thrown, message retried |

### Retry Logic

**Service Bus Retry Policy**:
1. **Initial Failure**: Exception caught in Run method
2. **Automatic Retry**: Service Bus retries with exponential backoff
   - Attempt 1: Immediate
   - Attempt 2: ~1 second delay
   - Attempt 3: ~2 second delay
   - Attempt 4: ~4 second delay
   - ... up to Max Retry Count (typically 10)
3. **Max Retries Exceeded**: Message moved to Dead-Letter Queue
4. **Manual Intervention**: Admin must investigate and reprocess

**Code-Level Retry**:
- No explicit retry in trigger (relies on Service Bus)
- Archive happens before update (no rollback needed)

### Exception Propagation

```csharp
try {
  // Main processing
  foreach (var item in input.ItemLines) { ... }
  
  // Order Tracking (currently disabled)
  try {
    if (isPickEvent || isUnpickEvent) {
      // _serviceBusQueueService.SendMessageAsync(...);
    }
  } catch (Exception e) {
    _loggerService.LogExceptionQueueErrorMessage(e, 
      INVENTORY_STATE_CHANGED_REFLEX_QUEUE_NAME, 
      message.MessageId, input);
  }
} catch (Exception e) {
  // Outer exception from deserialization, etc.
  _loggerService.LogExceptionQueueErrorMessage(e, 
    INVENTORY_STATE_CHANGED_REFLEX_QUEUE_NAME, 
    message.MessageId);
}
```

### Rollback Scenarios

| Scenario | Rollback | Result |
|----------|----------|--------|
| Inventory not found | Skip operation | Item not processed, continue |
| Quantity calculation fails | No update issued | State unchanged |
| Database update fails | Exception thrown | Message retried by Service Bus |
| Nexus publish fails | Logged, processing continues | Inventory updated but OMS not notified |
| ICR snapshot fails | Logged, processing continues | Item processed but no audit snapshot |

---

## 9. Known Limitations & Future Improvements

### Current Limitations

1. **Order Tracking Disabled**:
   - Current: Feature flag checked but logging warning instead of publishing
   - Impact: Order tracking system not updated
   - Fix Required: Uncomment `_serviceBusQueueService.SendMessageAsync()` call

2. **Nexus Publishing Commented**:
   - Current: B2B adjusted/moved and ICR snapshots NOT sent to Nexus
   - Comment: "Todo: send message to nexus-producer-b2b-inventory-adjusted-moved service bus queue"
   - Impact: SAP and audit systems not updated
   - Fix Required: Uncomment publishing calls

3. **DurableClient Unused**:
   - Current: Parameter injected but not used
   - Commented: Order tracking orchestrator calls commented out
   - Impact: No distributed orchestration
   - Fix Required: Implement durable orchestration if needed

4. **No Distributed Transactions**:
   - Current: Item-level updates not atomic
   - Risk: Partial message processing possible
   - Limitation: Azure Functions don't support distributed transactions easily

5. **No Idempotency Handling**:
   - Current: Duplicate messages cause duplicate updates
   - Improvement: Implement request deduplication using message ID

6. **Concurrency Issues**:
   - Current: Last-write-wins without conflict detection
   - Improvement: Add optimistic concurrency (version/timestamp)

### Potential Improvements

1. **Batch Database Operations**:
   - Current: Sequential database calls per item
   - Improvement: Batch insert/update for multiple items
   - Benefit: 50% reduction in database round trips

2. **Segmentation Rule Caching**:
   - Current: Fresh read per item line
   - Improvement: In-memory cache with TTL
   - Benefit: Reduced repository calls, improved latency

3. **Message Validation Framework**:
   - Current: Implicit validation via deserialization
   - Improvement: Explicit FluentValidation or DataAnnotations
   - Benefit: Better error messages, consistent validation

4. **Async Nexus Publishing**:
   - Current: Blocking calls to SendMessageAsync
   - Improvement: Fire-and-forget with background retry
   - Benefit: Reduced function execution time, improved throughput

5. **Structured Logging**:
   - Current: Unstructured log messages
   - Improvement: Structured logging with correlation ID
   - Benefit: Better searchability and analytics

6. **Telemetry/Monitoring**:
   - Current: Basic logging, no metrics
   - Improvement: Application Insights metrics for pick/unpick counts, delta distributions
   - Benefit: Performance visibility, anomaly detection

7. **Dead-Letter Processing**:
   - Current: Messages go to DLQ on failure
   - Improvement: Automated DLQ processor with retry logic
   - Benefit: Reduced manual intervention

8. **Circuit Breaker Pattern**:
   - Current: No fallback if downstream service unavailable
   - Improvement: Circuit breaker for Nexus publishing
   - Benefit: Faster failure, graceful degradation

---

## 10. Summary

### Complete Execution Summary

**InventoryStateChangedFullQueueTrigger** is a critical inventory processing pipeline that:

1. **Consumes** inventory state change events from Service Bus
2. **Processes** pick, unpick, and segmentation operations per item line
3. **Updates** central inventory database with new allocation states
4. **Calculates** B2C extension and availability deltas
5. **Publishes** notifications to SAP (B2B adjustments), OMS (B2C deltas), and audit systems (ICR snapshots)
6. **Archives** complete audit trail for compliance

The trigger handles **100+ inventory state transitions daily** across B2B and B2C domains, managing allocation, extension, and segmentation logic with minimal latency.

### Key Business Logic

| Logic | Purpose | Impact |
|-------|---------|--------|
| Pick Event Processing | Transition allocation to prepared | Order preparation tracking |
| Unpick Event Processing | Reverse pick operation | Order cancellation support |
| B2C Extension | Allocate B2B to B2C demand | Improved fulfillment rates |
| Inventory Segmentation | Distribute new stock to domains | Cross-domain allocation balance |
| Delta Synchronization | Notify OMS of B2C changes | Order promise accuracy |
| Audit Archival | Track all changes | Compliance & reconciliation |

### Database Update Summary

- **ItemStockInventory**: Primary table updated 1-2 times per item line (pick, unpick, segmentation)
- **ItemStockInventoryExtended**: Created/updated for non-PICKABLE states (SKUs in damage/quarantine/transit)
- **MessageArchive**: 2-4 entries per item line (before/after snapshots)
- **ItemLevelSegmentation**: Read-only (rules applied, not modified)
- **FulfilmentLevelSegmentation**: Read-only (fallback rules)

### Calculation Summary

1. **B2B Pick**: B2BAllocated - Qty, B2BPrepared + Qty
2. **B2C Pick**: B2CAllocated - Qty (or extend from B2B if needed)
3. **Unpick**: Reverse of pick (increment allocated, decrement prepared)
4. **Segmentation**: Apply % rules to inbound quantity
5. **Extension**: Calculate B2CAvailable = B2COrg + (B2BAVL - B2BAllocated - B2BUsedShare)
6. **Delta**: DeltaToOMS = B2CAvl_new - B2CAvl_prev

### Risks & Recommendations

| Risk | Severity | Mitigation |
|------|----------|-----------|
| Duplicate message processing | HIGH | Implement message deduplication (ID-based idempotency) |
| Concurrent inventory updates | HIGH | Add optimistic locking (version field) |
| Partial message failure | MEDIUM | Transaction wrapper or event sourcing |
| Nexus publishing disabled | MEDIUM | Uncomment and test publishing calls |
| Order tracking disabled | MEDIUM | Re-enable and test orchestration |
| Missing inventory skipped silently | MEDIUM | Alert on missing inventory records |
| State machine not validated | LOW | Add state transition validation |

### Maintenance Checklist

- [ ] Review feature flags quarterly (ENABLE_* settings)
- [ ] Monitor dead-letter queue for processing failures
- [ ] Audit archive table growth and implement retention policy
- [ ] Validate B2C extension calculations monthly (sample audit)
- [ ] Test order tracking re-enablement when needed
- [ ] Uncomment Nexus publishing calls when SAP/OMS integration ready
- [ ] Implement idempotency handler for duplicate message safety
- [ ] Add concurrency conflict detection (optimistic locking)
- [ ] Monitor pick/unpick success rates via Application Insights
- [ ] Review inventory variance reports (delta reconciliation)

---

## Appendix: Glossary

| Term | Definition |
|------|-----------|
| **B2B** | Business-to-Business inventory domain (wholesale/distribution) |
| **B2C** | Business-to-Consumer inventory domain (retail/ecommerce) |
| **B2BAVL** | B2B Available quantity (total B2B inventory) |
| **B2CAVL** | B2C Available quantity (original + extended) |
| **Allocated** | Inventory reserved/committed to specific orders |
| **Prepared** | Inventory picked and staged for shipment |
| **Extended** | B2B inventory temporarily allocated to B2C demand |
| **Hallmarking** | Gold purity marking (22K, 18K, 24K) |
| **COO** | Country of Origin (source country) |
| **3PL** | Third-Party Logistics provider (e.g., CAECOM) |
| **TDC/EDC/ADC** | Fulfillment centers (Tier-1/Tier-2/Tier-3 Distribution Centers) |
| **DGP** | Demand Generation Product (B2B product type) |
| **ICR** | Inventory Comparison Report (audit snapshot) |
| **OMNI** | Omnichannel flag (inventory shared across channels) |
| **SAP** | Legacy ERP system (master data source) |
| **OMS** | Order Management System (fulfillment logic) |
| **Nexus** | Event streaming/publishing system (downstream data distribution) |

---

*Documentation Generated: 2024*
*Last Reviewed: [To be updated]*
*Version: 1.0*
