# Shared helper — `SegmentInventoryHelper` (B2B/B2C segmentation)

> Consumers: segmented inventory events (InventoryStateChanged,
> OrderToInventoryAllocated, StockOnHandUpdated). Uses
> [b2c-extension-calculation.md](b2c-extension-calculation.md) for the extended
> path.

## Purpose

Distributes newly-inbound inventory across the B2B and B2C domains according to
fulfilment-level or item-level rules. Centralized so every segmenting event
applies the same rule precedence and delta calculation.

## Rule precedence (by location type)

| Location type | Strategy | Data source |
|---|---|---|
| `THIRD_PARTY_LOGISTICS` (3PL) | Fulfilment-level B2C segmentation | `FulfilmentLevelSegmentationRepository` |
| `WAREHOUSE` (default) | Item-level **if active**, else fulfilment-level | `ItemLevelSegmentationRepository` → `FulfilmentLevelSegmentationRepository` |

- **Item-level** rules take priority over fulfilment-level when present and
  `IsActive`; they carry item-specific B2C allocation % and
  `StoreLeveragePercentage`, and mark the aggregate `IsExtended` for delta
  calculation.
- **Fulfilment-level** applies a uniform B2C allocation % across items when no
  active item-level rule exists.

## Algorithm

```
1. Fetch ItemStockInventory (create with defaults if missing).
2. inboundQty = signed normalization (see inventory-formulas.md).
3. If inboundQty < 0 AND inventory was missing → fail (cannot negate empty).
4. Save previous B2CAVL and B2COrg for delta calculation.
5. Apply segmentation:
     if LocationType == 3PL: DoFulfilmentLevelB2CSegmentation(inboundQty)
     else if item-level rule active: IsExtended = true; DoItemLevelExtension(inboundQty, ecomShare%)
     else: DoFulfilmentLevelSegmentation(inboundQty)
6. delta = currentB2CAVL - previousB2CAVL
7. Archive updated inventory (see archive-audit.md).
8. Persist via Patch/ETag (see cosmos-idempotent-write.md).
9. Return response with IsB2CChanged flag + delta.
```

## Outputs

- Updated inventory aggregate (persisted via Patch `Increment`/`Set`).
- `IsB2CChanged` + signed delta → OMS publisher.

## Edge cases

- Missing item stock on a positive inbound → create with defaults.
- Missing item stock on a negative inbound → fail (step 3).
- No active item-level rule → fulfilment-level fallback, `IsExtended` stays
  false.

## Persistence & idempotency

All writes go through [cosmos-idempotent-write.md](cosmos-idempotent-write.md);
segmentation increments use `PatchOperation.Increment` so redelivery is safe.
