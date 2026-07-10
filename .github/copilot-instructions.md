# iis-wms-integrations — root instructions

## Project context

This service is an inventory/WMS integration platform. It consumes Kafka
inventory events, republishes them onto Azure Service Bus for durable
processing, persists state in Azure Cosmos DB, and exposes a REST API for
downstream consumers. It runs on AKS. See
[docs/ai/integration-resiliency.instructions.md](../docs/ai/integration-resiliency.instructions.md)
for the end-to-end data flow.

## Required reading

Read these documents before generating or reviewing code. Each owns a
distinct concern — do not restate one file's content in another. This
assumes tool access to read files; if you're ever operating without
file-read access, say explicitly that these docs couldn't be consulted
rather than proceeding as if you'd read them.

| # | Document | Owns |
|---|---|---|
| 1 | [docs/ai/engineering-standards.instructions.md](../docs/ai/engineering-standards.instructions.md) | Baseline tech stack, versions, cross-cutting standards (logging, security, coverage). Wins on any version/number conflict. |
| 2 | [docs/ai/dotnet-architecture-good-practices.instructions.md](../docs/ai/dotnet-architecture-good-practices.instructions.md) | Clean Architecture layering, DDD, SOLID (canonical definitions), test naming convention. |
| 3 | [docs/ai/csharp.instructions.md](../docs/ai/csharp.instructions.md) | C# language idioms and formatting only. |
| 4 | [docs/ai/oop-design-patterns.instructions.md](../docs/ai/oop-design-patterns.instructions.md) | GoF pattern selection. Defers to #2 for SOLID text and to #3 for doc-comment style. |
| 5 | [docs/ai/aspnet-rest-apis.instructions.md](../docs/ai/aspnet-rest-apis.instructions.md) | Web API layer: routing, versioning, validation, exception middleware wiring. |
| 6 | [docs/ai/cosmos-db.instructions.md](../docs/ai/cosmos-db.instructions.md) | Cosmos DB data access, concurrency, repository pattern. |
| 7 | [docs/ai/integration-resiliency.instructions.md](../docs/ai/integration-resiliency.instructions.md) | Kafka, Service Bus, Blob storage, Polly, correlation IDs, async/parallelism, test infrastructure. |
| 8 | [docs/ai/kubernetes-deployment-best-practices.instructions.md](../docs/ai/kubernetes-deployment-best-practices.instructions.md) | AKS deployment, health probes, autoscaling, secrets. |

Note: [docs/ai/skills-generation.instructions.md](../docs/ai/skills-generation.instructions.md)
is Claude Code-specific (it governs `.claude/skills/` generation) and has no
Copilot equivalent — it is intentionally not referenced here, and its
absence from this list is not a sync gap with
[CLAUDE.md](../CLAUDE.md).

## Precedence when documents disagree

1. **More specific beats more general.** A rule about Cosmos DB in
   `cosmos-db.instructions.md` wins over a general data-access remark
   elsewhere.
2. **`engineering-standards.instructions.md` wins on any version, threshold,
   or numeric conflict** (target framework, coverage %, TLS version, etc.).
3. **`dotnet-architecture-good-practices.instructions.md` is the sole source
   of truth for SOLID definitions and test naming** — other files reference
   it instead of restating it.
4. If a conflict doesn't fit the above and isn't resolved by this file, stop
   and ask rather than guessing which rule wins. **Running non-interactively
   with no one to ask?** Don't guess and don't skip the work silently either
   — implement the unambiguous parts, leave the conflicting part with a
   `// TODO(ai): unresolved precedence conflict — <the two rules and why>`
   marker (or the equivalent for the file type), and call it out plainly in
   your summary of the change so a human resolves it on review.

## If a referenced document is missing or unreadable

Stop and report it. Do not proceed on assumptions about its contents, and do
not silently skip it.

## Working rules

- Follow the documented architecture. Do not change public APIs without
  explaining why.
- Never introduce a new third-party library or NuGet package without
  approval — propose it and explain the tradeoff first, then wait for an
  explicit yes. Silence is not approval: if no one responds (e.g. a
  non-interactive run), don't add the dependency — implement against what's
  already named across these docs (`Confluent.Kafka`,
  `Azure.Messaging.ServiceBus`, `Microsoft.Azure.Cosmos`, `Polly.Core`,
  `FluentValidation`, `Serilog`, `xUnit`, and their transitive framework
  dependencies) — plus `MediatR`, used specifically for domain-event
  dispatch per
  [oop-design-patterns.instructions.md](../docs/ai/oop-design-patterns.instructions.md).
  Those are already approved by being specified here, so using them isn't
  "introducing" anything new.
- Prefer xUnit for tests (see
  [docs/ai/integration-resiliency.instructions.md](../docs/ai/integration-resiliency.instructions.md)
  for unit vs. integration test setup).
- Never commit secrets, connection strings, or keys. Local development uses
  user-secrets or the emulator; every other environment uses Managed
  Identity / Workload Identity against Azure Key Vault.
- Build and run the affected test suite before considering a change done.
  If you can't run it (no test project yet, missing tooling), say so
  explicitly rather than reporting success.
- **Strictly validate every implementation against current Microsoft best
  practices, and pick the optimized approach over the first one that just
  works.** Before treating a change as done, cross-check it against the
  relevant official Microsoft Learn / SDK guidance for the APIs touched
  (ASP.NET Core, C#/.NET, Azure SDKs) — "it compiles and the tests pass" is
  not sufficient on its own. Where more than one correct approach exists,
  prefer the more efficient/idiomatic one (e.g. avoid sync-over-async,
  avoid unnecessary allocations or abstraction layers, avoid a superseded
  API when a current one exists) over whichever was simplest to write. If
  the Microsoft-recommended pattern conflicts with a rule in one of the
  numbered docs above, the numbered doc wins (see Precedence) — call out
  the conflict in your summary instead of silently picking one. If you are
  not confident an approach matches current guidance, say so explicitly
  rather than asserting that it does.

## Keeping instructions in sync

This file and [CLAUDE.md](../CLAUDE.md) must stay consistent — they steer
different tools against the same codebase. If you change a rule in one,
mirror it in the other in the same change. **Exception:**
[docs/ai/skills-generation.instructions.md](../docs/ai/skills-generation.instructions.md)
is Claude Code-specific and intentionally not referenced from this file —
its absence here is not a sync gap.
