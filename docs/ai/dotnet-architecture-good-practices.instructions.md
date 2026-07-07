---
description: "DDD, SOLID, and Clean Architecture layering for this .NET inventory integration service. Canonical source for SOLID definitions and test naming — other docs reference this file instead of restating it."
applyTo: '**/*.cs,**/*.csproj,**/Program.cs'
---

# DDD & Clean Architecture Guidelines

This service ingests Kafka inventory events, republishes them to Azure
Service Bus, persists state in Azure Cosmos DB, and exposes a REST API. It
is an **inventory/warehouse management domain**, not a financial or payments
system — apply the domain checklist in §5 accordingly; do not import
generic financial-compliance requirements (PCI-DSS, SOX, card data rules)
that don't apply here.

The two sections immediately below are prerequisites for the numbered §1–§5
sequence that follows — they're intentionally unnumbered lead-in material
(the same pattern `cosmos-db.instructions.md` uses for its "AI code
generation rules"), not sections another doc would cross-reference by
number.

## Solution structure (canonical)

```
src/
├── Api/              # composition root: endpoints, middleware, DI wiring, Program.cs
├── Application/      # use cases, interfaces, DTOs, validators, mapping
├── Domain/           # aggregates, value objects, domain events, domain exceptions — no external references
└── Infrastructure/   # Cosmos DB, Service Bus, Kafka, Blob Storage adapters; implements Application/Domain interfaces
tests/
├── UnitTests/
└── IntegrationTests/
```

Dependencies flow inward only: `Api → Application → Domain`, with
`Infrastructure` implementing interfaces defined in `Application`/`Domain`
and depending on them, never the reverse. The Kafka consumer and Service Bus
consumer hosted services (see
[integration-resiliency.instructions.md](integration-resiliency.instructions.md))
live in `Infrastructure` and call into `Application` the same way an API
controller does — they are not a separate architecture or an exception to
this rule.

## When this file's process applies

The full analysis-and-confirmation process in §2 is required for changes
that touch the **Domain or Application** layer, or that introduce/modify an
aggregate, domain event, or business rule. For **Infrastructure-only**
changes (wiring, configuration, logging, a new Cosmos query, a retry policy
tweak) or documentation/comment-only changes, skip straight to
implementation — state in one sentence which layer the change touches and
why the full process doesn't apply, rather than running through it anyway.

**Mixed-layer changes run the full process.** A change that touches both
— e.g., a new Cosmos query added *because* a new business rule needs it —
is not "mostly Infrastructure." Classify any change that has a
Domain/Application-layer reason behind it as Domain/Application-touching,
even if most of the diff lands in `Infrastructure`. The cost of running the
full process on a change that turns out simple is small; the cost of
skipping it on a change that turns out to encode a business rule is a
silently under-tested aggregate. When genuinely unsure which side a change
falls on, default to running the full process rather than skipping it.

## 1. Core Principles

### Domain-Driven Design

* **Ubiquitous language**: consistent business terminology (`SKU`,
  `Reservation`, `Allocation`, `OnHandQuantity`) across code, tests, and
  docs.
* **Bounded contexts**: clear service boundaries with well-defined
  responsibilities.
* **Aggregates**: consistency boundaries and transactional integrity — an
  inventory aggregate is the unit that enforces "never let quantity go
  negative."
* **Domain events**: capture business-significant occurrences (`StockReserved`,
  `StockAllocated`, `StockAdjusted`) for audit and downstream consumers.
* **Rich domain models**: business logic lives in the domain layer, not in
  application services.

### SOLID Principles (canonical definitions — reference this section, don't restate it)

* **Single Responsibility (SRP)**: a class has only one reason to change.
  In this codebase: an `InventoryEventProcessor` that both applies stock
  mutations *and* formats the Blob Storage audit log has two reasons to
  change (a business-rule change, and an audit-format change) — split them
  into an aggregate method and a separate `IRequestAuditWriter`.
* **Open/Closed (OCP)**: open for extension, closed for modification.
* **Liskov Substitution (LSP)**: subtypes must be substitutable for their
  base types.
* **Interface Segregation (ISP)**: no client is forced to depend on methods
  it doesn't use. In this codebase: don't make `IInventoryEventRepository`
  (7 methods, [cosmos-db.instructions.md](cosmos-db.instructions.md) §5)
  the only interface a read-only reporting service depends on — extract a
  narrower `IInventoryEventReader` with just the two query methods
  (`GetPagedAsync`, `QueryAsync`).
* **Dependency Inversion (DIP)**: depend on abstractions, not concretions.
  In this codebase: the Kafka consumer, Service Bus consumer, and API
  controllers all depend on `IInventoryEventRepository`, never on
  `Microsoft.Azure.Cosmos.Container` directly — this is the concrete
  mechanism [cosmos-db.instructions.md](cosmos-db.instructions.md) §11
  ("API Boundary Rules") enforces.

### .NET Practices

* **Async**: `async`/`await` for all I/O; see
  [csharp.instructions.md](csharp.instructions.md) for the `CancellationToken`
  and naming rules.
* **DI**: constructor injection via the built-in container; see
  [cosmos-db.instructions.md](cosmos-db.instructions.md) §12 for the
  lifetime table (Singleton/Scoped) this service follows.
* **Exceptions**: throw domain-specific exceptions from the Domain layer
  (e.g., `InsufficientStockException`); translate to Problem Details only at
  the Api boundary (see
  [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md)).

## 2. Required process for Domain/Application changes

Before implementing, state:

1. **Analysis** — which DDD patterns/SOLID principles apply, which layer(s)
   are affected, how the change aligns with ubiquitous language.
2. **Design check** — does it respect aggregate boundaries and SRP; are
   domain rules encapsulated in the aggregate, not the application service.
3. **Plan** — which aggregates/entities change, what domain events fire,
   what tests are needed (naming per §4).

If you can't state these clearly, stop and ask rather than guessing.

After implementing, confirm each of the following in one line — this is
the single post-implementation checklist for this file (nothing later in
this doc restates it):

* Aggregate boundaries and invariants are respected.
* Tests follow `MethodName_Condition_ExpectedResult()` (§4) and cover the
  oversell/idempotency edge case (§5) if the change touches quantity.
* Domain events are published for business-significant state changes.
* `.NET` practices followed (async, DI, exception translation at the Api
  boundary only — see §1).

Skip any confirmation that's plainly not applicable to a trivial change
(e.g., don't fabricate a "domain events" confirmation for a change that
published none) — say "not applicable" and why, instead of forcing a
statement that doesn't fit. If any item can't be confirmed at all, say why
and ask rather than asserting it.

## 3. Layer responsibilities

### Domain
Aggregates, value objects, domain services (stateless, multi-aggregate
operations), domain events, specifications for complex query/business
rules. No dependency on Infrastructure, ASP.NET Core, or Cosmos SDK types.

### Application
Use-case orchestration, DTOs, input validation, interfaces that
Infrastructure implements. Constructor-injected dependencies only.

### Infrastructure
Repositories (Cosmos DB — see
[cosmos-db.instructions.md](cosmos-db.instructions.md)), the Kafka consumer
and Service Bus consumer/publisher, Blob Storage adapters, external service
clients. Implements interfaces defined in Application/Domain.

## 4. Testing standards

* **Naming**: `MethodName_Condition_ExpectedResult()` — this is the one
  naming convention for this codebase; no other file should suggest
  "match nearby files" as an alternative.
* **Unit tests**: domain logic and business rules in isolation, no I/O.
* **Integration tests**: aggregate persistence, Cosmos DB behavior
  (concurrency, patch semantics), Kafka/Service Bus message flow — see
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md).
* **Coverage**: 85% minimum on Domain and Application layers (canonical
  number lives in
  [engineering-standards.instructions.md](engineering-standards.instructions.md) §7).

```csharp
[Fact(DisplayName = "Descriptive test scenario")]
public void MethodName_Condition_ExpectedResult()
{
    // Arrange
    var aggregate = CreateTestAggregate();

    // Act
    var result = aggregate.PerformAction(parameters);

    // Assert
    Assert.Equal(expected, result.Value);
}
```

## 5. Inventory domain considerations

This replaces generic financial-compliance language with the concurrency
and consistency guarantees this specific domain actually needs:

* **Oversell prevention**: every stock decrement must be a conditional
  operation (ETag match, or a Cosmos Patch with a quantity-guard) — never a
  read-modify-write without a concurrency check. See
  [cosmos-db.instructions.md](cosmos-db.instructions.md) §9.
* **Idempotency**: Kafka and Service Bus messages can be redelivered.
  Aggregates that mutate quantity must be safe to apply twice for the same
  message ID (dedupe key, or a naturally idempotent operation like "set
  allocation for order X to Y" rather than "decrement by Y"). See
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md).
* **Eventual consistency across the pipeline**: Kafka → Service Bus →
  Cosmos DB is an eventually-consistent chain. Use domain events plus
  compensating actions (not two-phase commit) when a downstream step fails
  after an upstream step has committed — document the compensation path in
  the aggregate that owns it.
* **Reservation consistency**: a reservation and its eventual allocation or
  release must net to zero; model this as an explicit state machine on the
  aggregate, not as separate uncoordinated writes.
* **Monetary fields — forward-looking, not currently applicable**: this
  domain has no monetary fields today (no pricing, no payments). If one is
  added later (e.g., a unit-cost field for a reporting feature), use
  `decimal`, never `float`/`double`. This bullet exists so that *if* such a
  field ever appears, the type choice is already decided — it is not an
  invitation to add financial-compliance process; the opening note in this
  file still stands.
