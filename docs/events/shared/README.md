# Shared event-processing logic — prompt library

These prompts describe **reusable service / helper classes** referenced by the
event documentation under [`docs/events/`](../). Logic that is common to more
than one event is specified **once here** and linked from each event doc, so the
event docs stay focused on their own business rules.

Each file is written to the same standard as the event docs: it describes a
concrete class (purpose, inputs, processing, outputs, edge cases, and any
Cosmos DB / Service Bus interaction) so it can be implemented directly later.

## Canonical architecture (applies to every helper and every event)

All helpers assume the AKS hosted-service pipeline defined in
[integration-resiliency.instructions.md](../../ai/integration-resiliency.instructions.md)
and the Cosmos rules in
[cosmos-db.instructions.md](../../ai/cosmos-db.instructions.md):

```
Kafka topic  ─▶  KafkaConsumerHostedServiceBase (BackgroundService)
             ─▶  Azure Service Bus queue (session-enabled)
             ─▶  ServiceBusConsumerHostedService<TMessage>  (IIS.WMS.Common)
             ─▶  concrete …ServiceBusHostedService subclass
             ─▶  Application handler
             ─▶  Cosmos DB repository (ETag-guarded Patch) + Blob archive
             ─▶  outbound ServiceBusSender (downstream queues)
```

There are **no Azure Functions, Durable Functions, Orchestrators, or
`DurableTaskClient`** anywhere in this platform. Any such term in an older doc
is obsolete.

## Index

| File | Helper / service | Used by |
|---|---|---|
| [cosmos-idempotent-write.md](cosmos-idempotent-write.md) | Deterministic Id, 409-as-applied, ETag + Patch (≤10 ops), 412 re-read/reapply loop | all writing events |
| [service-bus-publishing.md](service-bus-publishing.md) | Cached `ServiceBusSender`, queue resolution, relay envelope, correlation headers | all events with outbound sends |
| [b2c-extension-calculation.md](b2c-extension-calculation.md) | `ExtendedInventoryHelper.CalculateB2CExtensionAsync` | StockOnHand, OrderToInventoryAllocated, StockSync |
| [inventory-formulas.md](inventory-formulas.md) | `FormulaHelper` (B2B available, B2C available), quantity normalization | most inventory events |
| [segment-inventory.md](segment-inventory.md) | `SegmentInventoryHelper` segmentation | segmented inventory events |
| [delta-towards-oms.md](delta-towards-oms.md) | Delta-towards-OMS + order-tracking request builder | GoodsInTransit, ConsolidatedOrderShipped, Hallmarking |
| [icr-snapshot.md](icr-snapshot.md) | ICR snapshot / `OmniInventoryAvailabilityReported` | StockSync, StockOnHand |
| [country-code-lookup.md](country-code-lookup.md) | Country-code resolution | several events |
| [archive-audit.md](archive-audit.md) | `ArchiveMessageAsync` blob/Cosmos archival | all events |

## Outbound queue names

Queue names are **kebab-case** and resolved from the `ServiceBus` configuration
section (never hard-coded). The old env-var constant is shown in parentheses for
traceability only:

| Config key / queue name | Old constant |
|---|---|
| `nexus-producer` | `NEXUS_PRODUCER_QUEUE_NAME` |
| `nexus-b2cstock-producer` | `NEXUS_B2CSTOCK_PRODUCER_QUEUE_NAME` |
| `order-tracking` | `ORDER_TRACKING_QUEUE_NAME` |
| `inventory-adjusted-reflex` | `INVENTORY_ADJUSTED_REFLEX_QUEUE_NAME` |
