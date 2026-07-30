# Shared helper — B2C extension calculation (`ExtendedInventoryHelper`)

> Consumers: StockOnHandUpdated, OrderToInventoryAllocated, StockSyncSubmitted,
> InventoryStateChanged. Depends on
> [inventory-formulas.md](inventory-formulas.md); emits the delta consumed by
> [delta-towards-oms.md](delta-towards-oms.md).

## Purpose

`ExtendedInventoryHelper.CalculateB2CExtensionAsync` intelligently allocates B2B
inventory into B2C when B2C demand exceeds its formal allocation — the "B2C
extension" mechanism. It centralizes the extension math and the resulting OMS
delta so multiple events behave identically.

## When triggered

- Pick on B2B inventory of an **extended** item.
- Pick on a B2C allocation **without sufficient allocated inventory** (needs
  extension).
- Unpick on extended inventory.
- Segmentation with item-level rules and the `IsExtended` flag set.

## Inputs

| Input | Source |
|---|---|
| Inventory aggregate (`B2BAVL`, `B2BAllocated`, `B2BUsedShare`, `B2COrg`, `B2CAVL`, `B2CAllocated`, `B2CPrepared`) | `ItemStockInventory` (Cosmos) |
| `StoreLeveragePercentage` | ItemLevelSegmentation → FulfilmentLevelSegmentation fallback |
| Pick/unpick quantity | event item line |

## Processing

```
1. If B2CAllocated < PickQuantity → extension needed:
     B2BStockRequired = PickQuantity - B2CAllocated
     B2BUsedShare    += B2BStockRequired
     B2CAllocated     = 0
     B2CPrepared     += PickQuantity
2. B2CExtended = CalculateActualB2BAvailable(inventory)   // = B2BAVL - B2BAllocated - B2BUsedShare
3. B2CAVL_new  = CalculateB2CAvl(inventory)               // = B2COrg + B2CExtended
4. DeltaToOMS  = B2CAVL_new - B2CAVL_prev
```

## Outputs

- Updated inventory field set (applied via Patch `Increment`/`Set`, ≤10 ops).
- `IsB2CChanged` flag.
- `DeltaTowardsOMS` (signed) consumed by the OMS delta publisher.

## Boundary conditions

- `B2CExtended` cannot exceed `B2BAVL - B2BAllocated`.
- `B2CAVL` recalculated only when the extension actually changes.
- Missing `StoreLeveragePercentage` → `0`.

## Worked example

```
PickQuantity=100, B2CAllocated=60, B2BAVL=500, B2BAllocated=200,
B2BUsedShare=0, B2COrg=60, B2CAVL_prev=60

B2BStockRequired = 100-60 = 40
B2BUsedShare     = 40
B2CPrepared      = 100
B2CExtended      = 500-200-40 = 260
B2CAVL_new       = 60+260 = 320
DeltaToOMS       = 320-60 = +260   → OMS notified B2C available increased by 260
```

## Persistence & idempotency

Field mutations are applied via [cosmos-idempotent-write.md](cosmos-idempotent-write.md)
(deterministic Id, ETag, Patch `Increment`) so a redelivered event does not
double-apply the extension. Never last-write-wins any of these quantity fields.
