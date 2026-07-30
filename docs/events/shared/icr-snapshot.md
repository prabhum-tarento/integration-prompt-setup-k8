# Shared helper — ICR snapshot (`OmniInventoryAvailabilityReported`)

> Consumers: StockSyncSubmitted, StockOnHandUpdated, InventoryStateChanged.
> Publishes via [service-bus-publishing.md](service-bus-publishing.md).

## Purpose

Builds an Inventory Comparison Report (ICR) snapshot of an aggregate's full
availability across domains and states, for auditing and reconciliation, and
publishes it as an `OmniInventoryAvailabilityReported` event.

## Trigger

When `ENABLE_SNAPSHOT_FOR_ICR` is enabled.

## Processing

```
1. Fetch ItemStockInventory (if missing → skip, return empty).
2. Capture all allocation states:
     B2B Available  (AVAILABLE, PICKABLE)
     B2C Available  (B2COrg if IsExtended, else B2CAVL)
     B2B Prepared   (AVAILABLE, PREPARED)
     B2C Prepared   (AVAILABLE, PREPARED)
3. Map to OmniInventoryAvailabilityReported:
     ProductId, CountryOfOrigin, Hallmarking
     Location (3PL type detection for CAECOM)
     QuantityDetails: [B2B_AVL, B2C_AVL, B2B_PREP, B2C_PREP]
     ReportDate (supplied by caller, UTC)
     ProductUnits (from config)
4. Wrap with type Inventory_OmniInventoryAvailabilityReported.
5. Publish via cached ServiceBusSender (service-bus-publish pipeline).
```

## Data-accuracy rule

- **Use `B2COrg` when `IsExtended`** (reports the original allocation under
  extension), otherwise **use `B2CAVL`** (effective availability).
- Captures both domains (B2B, B2C) and both states (AVAILABLE, PREPARED) so the
  snapshot reconciles against source-of-truth.

## Identity & ordering

- Deterministic message id (see
  [service-bus-publishing.md](service-bus-publishing.md)); a redelivered
  snapshot is de-duplicated downstream, not double-reported.
- Publish **after** the state change is durably persisted.

## Edge cases

- Missing inventory record → skip snapshot (nothing to report), log at
  information.
- CAECOM location → mark as 3PL type in the reported `Location`.
