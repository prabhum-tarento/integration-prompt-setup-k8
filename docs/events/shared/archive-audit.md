# Shared helper — message archival & audit (`ArchiveMessageAsync`)

> Consumers: all events. Backed by `MessageArchiveRepository` (Cosmos DB,
> optionally mirrored to Azure Blob Storage cold tier). Uses the Polly
> `blob-upload` pipeline (see
> [integration-resiliency.instructions.md](../../ai/integration-resiliency.instructions.md) §3).

## Purpose

Persists a historical snapshot of the inbound message and/or the pre-mutation
inventory state for audit, reconciliation, and ICR, so every event archives the
same way instead of each re-describing it.

## Two archival points

1. **Cold-tier request audit (unconditional):** every consumed message is
   written to the request-audit container / cold-tier blob as received — this is
   best-effort and runs regardless of downstream outcome.
2. **State-change archive:** before/after an inventory mutation, the affected
   aggregate snapshot is archived so a delta can be reconstructed.

## Mechanism

- Cosmos archive via `MessageArchiveRepository`.
- Cold-tier mirror to Blob Storage when `Audit:ColdStorageEnabled` is set;
  uploads go through the `blob-upload` Polly pipeline (transient blob faults
  retried; **not** Cosmos exceptions).
- Deterministic archive id derived from the source event so a redelivered
  message does not create duplicate archive rows.

## Rules

- **Best-effort, non-blocking:** an archive/blob failure is logged (`Critical`
  for the unconditional audit path) and does **not** by itself fail the
  message; the message's own outcome is determined by its processing result.
- **Never contains secrets:** archived payloads carry business data only.

## Edge cases

- Blob upload fails after retries → logged, message continues (audit is
  best-effort).
- Oversized payload → claim-check offload to the large-payload container
  (handled by the relay, see
  [service-bus-publishing.md](service-bus-publishing.md)).
