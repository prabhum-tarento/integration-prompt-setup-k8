---
description: 'Baseline technical standards, versions, and cross-cutting rules for all .NET Web API services in this organization. Wins on any version/threshold conflict with a sibling doc.'
applyTo: '**/*.cs, **/*.csproj, **/appsettings*.json, **/*.yaml, **/*.yml, **/*.bicep'
---

# Engineering Standards: .NET 10 Web API

This document defines the baseline technical standards, architectural rules,
and coding conventions for all .NET 10 Web API services across the
organization, including this repository's Kafka → Service Bus → Cosmos DB
inventory integration service.

This file is the canonical source for **versions, numeric thresholds, and
tooling choices**. Where another `docs/ai/*.instructions.md` file states a
different number or version for the same thing, this file wins — fix the
other file rather than treating the conflict as open.

---

## 1. Tech Stack & Environment

* **Target Framework:** `.NET 10.0 (LTS)`
* **Language Version:** `C# 14`
* **API style:** ASP.NET Core Web API with Controllers (see
  [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md) for
  the rationale — this service integrates with typed client generators and
  API versioning tooling that assume controller/attribute routing).
* **ORM:** Entity Framework Core 10 is used only if a relational store is
  introduced. The primary store is Azure Cosmos DB accessed via the native
  `Microsoft.Azure.Cosmos` SDK — see
  [cosmos-db.instructions.md](cosmos-db.instructions.md) for why EF Core's
  Cosmos provider is not used (it doesn't support the Patch API or full
  ETag concurrency control that this service depends on).
* **API Documentation:** OpenAPI 3.1 via Microsoft's built-in generator +
  `Scalar.AspNetCore` UI.
* **Dependency management:** Central Package Management (CPM) via a root
  `Directory.Packages.props` file — every package version below is pinned
  there, not per-project.
* **Integration SDK versions** (this is the most version-sensitive part of
  the stack — pin these explicitly rather than floating):
  * `Confluent.Kafka`: latest stable 2.x compatible with the deployed
    broker's protocol version.
  * `Azure.Messaging.ServiceBus`: latest stable 7.x.
  * `Microsoft.Azure.Cosmos`: latest stable 3.x.
  * `Polly.Core`: latest stable 8.x (the `ResiliencePipelineBuilder` API in
    [integration-resiliency.instructions.md](integration-resiliency.instructions.md) §3
    requires v8 — v7's `Policy` API is a different, older shape).
  * `MediatR`: latest stable version, used for domain-event dispatch per
    [oop-design-patterns.instructions.md](oop-design-patterns.instructions.md) —
    check its current license terms before bumping major versions.
  * Bump policy: minor/patch versions on a monthly cadence via a dependency
    bot (Renovate/Dependabot) with CI green as the merge gate; major
    versions require an explicit review of that SDK's breaking-change notes
    before bumping `Directory.Packages.props`.

---

## 2. Solution & Architectural Blueprint

Strict **Clean Architecture**: four explicit boundaries, dependencies flow
inward only. Full layering rules, DDD guidance, and SOLID definitions live
in
[dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md) —
this section only states the dependency rule everything else assumes.

* The **Domain** layer has no references to external packages or other
  internal projects.
* The **Api** layer is the composition root; it maps endpoints and wires DI,
  and contains no business logic.
* Two hosted services (Kafka consumer, Service Bus consumer — see
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md))
  live in **Infrastructure** and call into **Application** exactly like an
  API controller does; they are not a separate architecture.

---

## 3. C# 14 & .NET 10 Coding Style

Full style rules live in
[csharp.instructions.md](csharp.instructions.md). This section states only
the project-wide compiler/tooling configuration:

* Enable strict nullability globally: `<Nullable>enable</Nullable>` and
  `<WarningsAsErrors>Nullable</WarningsAsErrors>`.
* Shared `.editorconfig` at the repo root; do not override it per-project.
* Every asynchronous method name ends with `Async` and forwards a
  `CancellationToken` to every child operation it awaits.

---

## 4. API Design Standards

* **Resource naming:** endpoints map to plural nouns (e.g., `/api/v1/inventory-events`), never verbs.
* **Versioning:** mandatory from day one, embedded in the URL path
  (`/api/v{version}/...`). See
  [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md).
* **Bulk/complex reads:** the HTTP `QUERY` method (safe, idempotent reads
  with a request body) is still an IETF draft
  (`draft-ietf-httpbis-safe-method-w-body`), not a ratified RFC — do not
  cite it as one. Until mainstream client tooling supports `QUERY`, expose
  the same semantics as `POST /api/v{version}/{resource}/search` returning
  identical response shapes, and migrate the route when `QUERY` support
  lands in ASP.NET Core and the org's HTTP client libraries.
* **Standardized errors:** RFC 9457 Problem Details via
  `builder.Services.AddProblemDetails()` and a global exception handler (see
  [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md)).
  Never return `200 OK` for a failed operation.
* **Validation:** FluentValidation for request DTOs.

---

## 5. Persistence Standards

* **Cosmos DB concurrency:** guard every write against concurrent
  modification using ETag-based optimistic concurrency
  (`IfMatchEtag` / `PatchItemRequestOptions.IfMatchEtag`) — see
  [cosmos-db.instructions.md](cosmos-db.instructions.md) §9 for the pattern.
  Do not use last-write-wins for any entity that supports inventory
  quantity mutation.
* **Tracking:** if EF Core is introduced for a relational store, default all
  read-only queries to `.AsNoTracking()`. Only fetch a tracked entity when
  it will be mutated and saved within the same `DbContext` scope.
* **Audit fields:** every persisted entity carries `CreatedUtc` and
  `ModifiedUtc`.

---

## 6. Security & Observability

* **Transport:** TLS 1.3 minimum on every inbound and outbound *network*
  channel this service actually has: client → Ingress (terminated at the
  Ingress controller, per
  [kubernetes-deployment-best-practices.instructions.md](kubernetes-deployment-best-practices.instructions.md)),
  and every outbound connection to Kafka, Service Bus, Cosmos DB, and Blob
  Storage (all already TLS/AMQPS-over-TLS at the protocol level via their
  respective SDKs — nothing to configure beyond not disabling it). This
  topology has **no pod-to-pod HTTP traffic** — the three workloads
  communicate exclusively through Kafka/Service Bus, not by calling each
  other's HTTP endpoints — so there is no unencrypted internal hop this
  rule needs to close today. If a future change adds direct service-to-
  service HTTP calls, that traffic must also be TLS (via a service mesh's
  mTLS or per-call HTTPS), and this bullet should be revisited at that
  point; don't add mesh-grade mTLS speculatively for a hop that doesn't
  exist yet.
* **AuthN/AuthZ:** stateless JWT Bearer (Microsoft Entra ID) for inbound API
  calls.
* **Secrets:** never in source, appsettings, or environment variable
  defaults. Local development uses .NET user-secrets or the Cosmos/Service
  Bus/Kafka emulators. Every other environment authenticates via Managed
  Identity / AKS Workload Identity against Azure Key Vault — see
  [kubernetes-deployment-best-practices.instructions.md](kubernetes-deployment-best-practices.instructions.md).
* **Telemetry:** OpenTelemetry for metrics and distributed traces; New
  Relic (or the configured OTLP backend) as the export target.
* **Structured logging:** Serilog, JSON output, every log entry carries the
  `CorrelationId` described in
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md) —
  that ID is generated (or read from an inbound header) at the HTTP or
  message-consumer boundary and flows through Kafka → Service Bus →
  Cosmos DB writes → response.
* **Log-level coverage is a gap-filling expectation, not a rewrite
  mandate.** A class in Application/Infrastructure/Api that performs I/O,
  makes a business decision, or can fail should log at more than one level:
  `Debug` on entry to a non-trivial method (with its key parameters),
  `Information` on a meaningful outcome (a write succeeded, a message was
  processed, a client was registered), and `Warning` on a recoverable or
  unexpected condition that isn't yet an error (a retry, a rejected
  cross-partition query, a failed health check). Add the missing level(s)
  only to a class that currently has none of this — don't touch a class
  that already logs meaningfully to fit this template exactly (the Kafka/
  Service Bus hosted services in
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md) §7
  and the `GlobalExceptionHandler` in
  [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md) are
  the reference examples of "already adequate," not a pattern to
  mechanically re-apply everywhere). Domain never takes a logging
  dependency — it has no external package references (§2) — so this
  applies only to Application, Infrastructure, and Api.

---

## 7. Testing Paradigm

* **Unit tests:** xUnit, isolated, no I/O — Domain and Application layers.
* **Integration tests:** `Microsoft.AspNetCore.Mvc.Testing` +
  `Testcontainers` (Cosmos DB emulator, Kafka, Service Bus emulator where
  available) — see
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md).
* **Coverage:** minimum **85% line coverage on the Domain and Application
  layers**, enforced in CI before merge to the default branch. This is the
  one number every sibling doc must match; if you see a different
  percentage or a different layer scope anywhere else, that other doc is
  wrong and should be corrected to match this line.
* **Infrastructure layer coverage:** minimum **70%**, measured across unit
  *and* integration tests combined — not unit tests alone. Infrastructure
  (the Cosmos repository, the Kafka/Service Bus consumers, Blob Storage
  adapters) is exactly the part of this service where mocking every Azure
  SDK call to hit a unit-test number adds little real assurance; the
  Testcontainers-based integration tests are expected to carry most of this
  layer's coverage. Specifically: the ETag concurrency retry, patch
  operation limits, and the cross-partition guardrail must be exercised by
  a real test per
  [cosmos-db.instructions.md](cosmos-db.instructions.md) §13, and the full
  Kafka → Service Bus → Cosmos DB flow (including the redelivery/dedupe
  case) per
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md) §9 —
  neither is satisfied by inspection alone.
