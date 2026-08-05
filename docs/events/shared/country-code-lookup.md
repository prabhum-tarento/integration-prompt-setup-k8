# Shared helper — country-code lookup

> Consumers: several events building OMS/SAP requests. Backed by
> `FulfilmentUnitRepository` for resolution and `CountryRepository` for
> validation (both Cosmos DB, read-only).

## Purpose

Resolves a market code for an event from `FulfilmentUnitRepository`, with a
safe fallback, so downstream requests (delta-towards-OMS, ICR) always carry a
valid market value. `CountryRepository` then validates that resolved code
against `CountryMaster` master data as a non-fatal observability check — it
never changes the resolved market.

## Processing

```
1. Fetch FulfilmentUnit.CountryCode from FulfilmentUnitRepository by FulfilmentId.
2. On miss → market = "UNKNOWN".
3. If a market was resolved (not "UNKNOWN"), look it up in CountryRepository.
4. If missing or IsActive = false → log a warning (message still publishes with
   the resolved market unchanged).
```

## Rules

- **Fail-safe, never fatal:** a `FulfilmentUnitRepository` miss resolves to
  `"UNKNOWN"`; a `CountryRepository` miss/inactive result only logs a warning —
  neither ever fails the message.
- **Read-only:** this helper never writes; no ETag concerns.
- **Cached:** country mappings change rarely; a lookup may be cached per
  `FulfilmentId` for the process lifetime to avoid a Cosmos read per event.

## Edge cases

| Case | Result |
|---|---|
| `FulfilmentId` not in `FulfilmentUnitRepository` | market = `"UNKNOWN"`, `CountryRepository` lookup skipped |
| Resolved market not in `CountryRepository`, or `IsActive = false` | market published unchanged + warning logged |
| `FulfilmentUnitRepository` unavailable | market = `"UNKNOWN"` (fail-open) |

## `CountryRepository` validation step

Enrichment only, not a replacement for `FulfilmentUnitRepository`:
`CountryMaster` is keyed by country `Code`/`RegionCode`, not by fulfilment/
location id, so it cannot itself resolve a market from a `FulfilmentId` — it
can only confirm that an already-resolved market code corresponds to an
active, known country. See `DeltaTowardsOmsPublisher`
(§3.7, [inventory.InventoryStateChanged.md](../inventory.InventoryStateChanged.md)).
