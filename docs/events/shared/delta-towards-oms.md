# Shared helper — delta-towards-OMS + order-tracking request builder

> Consumers: GoodsInTransitReceived, ConsolidatedOrderShipped,
> InternalHallmarkingStatusChanged, OrderToInventoryAllocated,
> InventoryStateChanged. Publishes via
> [service-bus-publishing.md](service-bus-publishing.md); uses
> [country-code-lookup.md](country-code-lookup.md).

## Purpose

Builds and publishes the two related downstream notifications that tell OMS
about B2C availability changes and order-tracking state:

1. **`DeltaTowardsOmsEventRequest`** — signed B2C availability delta.
2. **Order-tracking request** — the common request formerly built by an
   Orchestrator; here it is a plain builder (`OrderTrackingCommonRequest`
   builder), **no Durable Task / Orchestrator involved**.

> The old `OrderTrackingCommonOrchestratorRequest` name is retained only for
> traceability — there is no orchestrator; it is a request DTO built in-process
> and published to the `order-tracking` queue.

## Delta-towards-OMS request

Built when `IsB2CChanged` is true and the relevant delta feature flag is
enabled.

```
DeltaTowardsOmsEventRequest {
  ReferenceId:      deterministic id (WarehouseId:Sku:EventId), not a fresh GUID
  ProductId:        ItemCode
  Location:         (Id, Type) from event
  Reason:           ReasonCode.ADJUSTMENT
  AdjustmentDate:   supplied by caller (UTC)
  ProductUnits:     "N/A"
  Market:           CountryCode (see country-code-lookup.md)
  QuantityDetails:  [{ CountryOfOrigin, Hallmarking, Quantity = signed delta,
                       State = (AVAILABLE, PICKABLE), ReasonTexts: [] }]
}
```

> **Deterministic `ReferenceId`.** The old code used
> `Guid.NewGuid()` per publish, which defeated downstream dedup and contributed
> to double-counting. Use a deterministic id derived from the source event so a
> redelivered publish is de-duplicated (see
> [service-bus-publishing.md](service-bus-publishing.md)).

Publishing: wrap in the downstream request envelope with type
`Inventory_B2CInventoryAdjusted` and publish to the `order-tracking` /
`nexus-producer` queue as the event requires, via the cached `ServiceBusSender`
and the `service-bus-publish` Polly pipeline.

## Trigger conditions (B2B adjusted/moved → SAP path)

Publish a B2B adjusted/moved event when **not** a pick/unpick and one of:

```
ENABLE_DELTA_TOWARDS_SAP     AND Location ∉ {EDC, ADC}
ENABLE_DELTA_TOWARDS_AX12_3PL AND Location == CAECOM
ENABLE_ADC_DELTA_TOWARDS_AX12 AND Location == ADC
```

With the historical fixes preserved:
- **SAE-2798:** if not a `B2B_INVENTORY_ADJUSTED` type and
  `FromState.State == ToState.State` and neither is `AVAILABLE` → skip (invalid
  transition).
- **SAE-3032:** if `FromState.State != AVAILABLE` → `FromState.Status = UNKNOWN`;
  same for `ToState`.
- **Quantity normalization:** negative adjustment lines → `Math.Abs` (see
  [inventory-formulas.md](inventory-formulas.md)).

## Failure handling

- Country lookup fails → fall back to `CountryCode.UNKNOWN`.
- `IsB2CChanged` false → skip publishing (conserve queue traffic).
- Feature flag disabled → skip with an information log.
- Publish transient failure → retried by the `service-bus-publish` pipeline;
  exhausted retries surface as a processing failure (outcome mapping in
  [cosmos-idempotent-write.md](cosmos-idempotent-write.md)).

## Ordering

Publish **after** the Cosmos state change is durably applied, so the delta
reflects committed state.
