# Shared helper — country-code lookup

> Consumers: several events building OMS/SAP requests. Backed by
> `CountryRepository` (Cosmos DB, read-only).

## Purpose

Resolves a market/`CountryCode` for an event from the `CountryRepository`, with
a safe fallback, so downstream requests (delta-towards-OMS, ICR) always carry a
valid market value.

## Processing

```
1. Fetch CountryCode from CountryRepository by FulfilmentId.
2. Try parse as the CountryCode enum.
3. On miss / parse failure → CountryCode.UNKNOWN.
```

## Rules

- **Fail-safe, never fatal:** a repository miss or invalid value resolves to
  `CountryCode.UNKNOWN` and logs a warning — it does not fail the message.
- **Read-only:** this helper never writes; no ETag concerns.
- **Cached:** country mappings change rarely; a lookup may be cached per
  `FulfilmentId` for the process lifetime to avoid a Cosmos read per event.

## Edge cases

| Case | Result |
|---|---|
| `FulfilmentId` not in repository | `CountryCode.UNKNOWN` + warning |
| Value present but not a valid enum member | `CountryCode.UNKNOWN` + warning |
| Repository unavailable | `CountryCode.UNKNOWN` + warning (fail-open) |
