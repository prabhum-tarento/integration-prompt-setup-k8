# Shared helper — Cosmos idempotent write (deterministic Id + ETag + Patch)

> **Canonical sources:** [cosmos-db.instructions.md](../../ai/cosmos-db.instructions.md)
> §5, §5a, §9, §10 and
> [integration-resiliency.instructions.md](../../ai/integration-resiliency.instructions.md)
> §2. These win over any event doc on conflict.

## Purpose

Every event that mutates inventory state in Cosmos DB shares one write
discipline whose job is to make an **at-least-once** Kafka → Service Bus
pipeline safe: a redelivered or concurrently-processed message must never
create a duplicate document or double-count a quantity. This helper describes
that discipline so each event doc references it instead of restating it.

This is the fix for the production symptom **"duplicate entry and doubling the
qty in Cosmos DB"**: it came from creating items with a fresh `Guid.NewGuid()`
Id per delivery and from last-write-wins replaces. Both are prohibited here.

## Design (three layers)

### 1. Deterministic document `Id`

`Id` is **derived from the source event** — the Kafka message key or a stable
payload field (e.g. `WarehouseId:Sku:EventId`) — **never `Guid.NewGuid()`**.
A duplicate delivery therefore targets the *same* item id both times.

### 2. `CreateAsync` → treat `409 Conflict` as "already applied"

For first-write (insert) paths, a redelivered `CreateAsync` hits the existing
deterministic id and Cosmos returns `409 Conflict`. The repository catches that
specific status and returns the existing item — the redelivered create is a
**no-op, not a failure** (cosmos §5):

```csharp
try
{
    var response = await container.CreateItemAsync(entity,
        new PartitionKey(entity.Category), cancellationToken: cancellationToken);
    return response.Resource;
}
catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
{
    return await GetAsync(entity.Id, entity.Category, cancellationToken)
        ?? throw new InvalidOperationException(
            $"Create conflicted on id {entity.Id} but the item could not be re-read.");
}
```

> `409 Conflict` (duplicate id) is distinct from `412 PreconditionFailed`
> (stale ETag). Do not conflate them. Also do not conflate either with
> **application-level** validation codes some events return (e.g.
> `MISSING_INVENTORY`, `INVALID_QUANTITY`) — those are business rejections, not
> Cosmos concurrency signals.

### 3. ETag + Patch for every mutation of an existing item

- **Prefer `PatchAsync` over `ReplaceAsync`.** Patch mutates only the fields
  this writer owns, so the document can be safely shared across applications
  without clobbering fields another writer set. Use `PatchOperation.Increment`
  for quantities (never read-modify-write-replace) and `PatchOperation.Set` for
  scalar state.
- **Hard limit: ≤10 patch operations per request.** The repository validates
  this and throws `ArgumentException` before calling Cosmos.
- **Always pass the current ETag** via `PatchItemRequestOptions.IfMatchEtag`
  (and `ItemRequestOptions.IfMatchEtag` for the rare genuine replace). A stale
  ETag yields `412 PreconditionFailed` → the repository throws
  `ConcurrencyException`.
- **Never last-write-wins** for any quantity, reservation, or allocation field.

```csharp
await repository.PatchAsync(
    entity.Id, entity.Category, entity.ETag!,
    [
        PatchOperation.Increment("/OnHandQuantity", delta),
        PatchOperation.Set("/ModifiedUtc", modifiedUtc),
    ],
    cancellationToken);
```

## Concurrency retry — bounded re-read-and-reapply loop

A `412`/`ConcurrencyException` on a message-driven write means another writer
updated the aggregate between our read and our write. **Re-read fresh state and
reapply** (integration §2) — this lives in the handler/use-case, not in Polly,
because it must re-fetch between attempts:

```csharp
const int maxAttempts = 3;

for (var attempt = 1; attempt <= maxAttempts; attempt++)
{
    var current = await repository.GetAsync(id, category, cancellationToken);
    try
    {
        await repository.PatchAsync(id, category, current!.ETag!, operations, cancellationToken);
        break;
    }
    catch (ConcurrencyException) when (attempt < maxAttempts)
    {
        continue; // reapply against the fresh ETag
    }
    catch (ConcurrencyException)
    {
        throw;    // exhausted retries → processing failure for this message
    }
}
```

## Message outcome mapping (definitive)

A `ConcurrencyException` escaping the loop propagates to the consumer's outer
handler, which maps outcomes exactly as follows (integration §2 — do not
restate or diverge elsewhere):

| Result | Service Bus action |
|---|---|
| No exception | `Completed` (`CompleteMessageAsync`) |
| `ConcurrencyException` | `Abandoned` (`AbandonMessageAsync`, retried to `MaxDeliveryCount`) |
| `OperationCanceledException` | `Abandoned` |
| Any other exception | `DeadLettered` (`DeadLetterMessageAsync`; `Reason` = type name, `Description` = `ex.ToString()`), after writing the payload to the hot-tier dead-letter container |

## Partition key / SessionId

The Cosmos partition-key property is `Category`; its **value** is the composite
`{WarehouseId}:{Sku}` (or the event's full composite key, e.g.
`FulfilmentId:ItemCode:Hallmark:CountryOfOrigin` for item stock). The Service
Bus `SessionId` uses the **same** composite so ordering and partitioning share
one key (integration §1, cosmos §4).

## Multi-container item stock (cosmos §5a)

Where an event writes item stock, it goes through `ItemStockInventoryRepository`,
the sanctioned multi-container repository serving `ItemStockInventoryEDC` / `TDC`
/ `ADC` / `CAECOM` / `BRZ3PL` (one per fulfilment code), resolved per call from
the `category`'s fulfilment-code segment. Never use a bare container string.

## Edge cases

- **Increment underflow / negative result:** validate the resulting quantity at
  the application layer before patching; a negative on-hand is a business
  rejection, not a Cosmos error.
- **Missing item on patch path:** a `404` means the create path should have run
  first — surface it, don't silently create with a replace.
- **ETag null:** never patch/replace without an ETag; if none can be produced,
  stop and flag rather than doing a blind write (cosmos §9 rule 3).
