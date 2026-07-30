# Shared helper — `FormulaHelper` (inventory quantity formulas)

> Consumers: most inventory + B2B events. Cross-references
> [b2c-extension-calculation.md](b2c-extension-calculation.md) and
> [delta-towards-oms.md](delta-towards-oms.md).

## Purpose

A stateless calculation helper for the inventory quantity formulas reused
across events, so the arithmetic is specified and unit-tested once. It performs
**no I/O** — callers supply a populated inventory aggregate and receive computed
values.

## Formulas

### Inbound quantity (signed normalization)

```
inboundQty = Convert.ToInt32(MoveSign + Quantity.ToString())
```

- `MoveSign` ∈ {`+`, `-`, empty}; `Quantity` is the unsigned magnitude.
- Result is a signed int: a deduction arrives as a negative `inboundQty`.
- **Null/empty `MoveSign`** → treat as `+`.

### Actual B2B available

```
CalculateActualB2BAvailable(inventory) = B2BAVL - B2BAllocated - B2BUsedShare
```

Represents true, unencumbered B2B stock — the ceiling for any B2C extension.

### B2C available

```
CalculateB2CAvl(inventory) = B2COrg + B2CExtended
```

Total B2C availability = original B2C allocation plus any B2B stock temporarily
extended into B2C.

## Quantity normalization (TDC / SAP)

- **Negative adjustment lines → absolute value** (`Math.Abs`) before publishing
  a B2B adjusted/moved event to SAP; SAP expects unsigned magnitudes with the
  direction carried separately.
- TDC-SAP source quantities are normalized to the service's internal signed
  convention on ingress.

## Boundary conditions & null handling

| Condition | Rule |
|---|---|
| `inboundQty < 0` and inventory record is null | Fail — cannot negate empty inventory |
| Missing `StoreLeveragePercentage` | Default to `0` |
| `B2CExtended` would exceed `B2BAVL - B2BAllocated` | Clamp at that ceiling |
| Any resulting quantity negative | Business rejection, not a Cosmos error |
| Missing numeric field | Default to `0`, never null-propagate into arithmetic |

## Worked example

```
B2BAVL=500, B2BAllocated=200, B2BUsedShare=40, B2COrg=60
ActualB2BAvailable = 500 - 200 - 40 = 260
B2CExtended        = 260 (within ceiling 500-200=300)
B2CAvl             = 60 + 260 = 320
```

## Notes

- Pure/synchronous; no Cosmos, no Service Bus, no clock. `AdjustmentDate` /
  `ReportDate` are supplied by callers (do not read the clock here) so results
  are deterministic and testable.
- Increments computed here are applied through
  [cosmos-idempotent-write.md](cosmos-idempotent-write.md) using
  `PatchOperation.Increment`, never a read-modify-write replace.
