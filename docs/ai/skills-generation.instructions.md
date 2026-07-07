---
description: 'When and how to create Claude Code Skills for this repo — recurring, multi-step engineering workflows specific to this Kafka/Service Bus/Cosmos DB integration service.'
applyTo: '.claude/skills/**/*.md'
---

# Claude Code Skills for This Repo

**This file is Claude Code-specific.** Skills (`.claude/skills/`) are a
Claude Code feature with no GitHub Copilot equivalent — this doc is
intentionally not referenced from
[.github/copilot-instructions.md](../../.github/copilot-instructions.md),
and that's not an oversync bug; don't "fix" it by adding a Skills section
there.

No `.claude/skills/` directory exists in this repo yet. This file governs
what goes in it once one is created — it is not itself a skill.

## When to create a Skill

Create one when a task in this repo is: **multi-step, recurring, and would
otherwise require re-deriving the same sequence from the `docs/ai/*`
instruction files every time.** Examples specific to this service:

* **Add a new inventory event type end-to-end** — Domain aggregate/value
  objects, the Cosmos entity and repository method
  ([cosmos-db.instructions.md](cosmos-db.instructions.md)), the Kafka →
  Service Bus mapping and `SessionId` assignment
  ([integration-resiliency.instructions.md](integration-resiliency.instructions.md) §1),
  the Application use case, the controller endpoint
  ([aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md)),
  and tests at every layer. Doing this by hand means re-reading five
  instruction files each time; a Skill walks the same checklist
  consistently.
* **Add a new consumer workload** — a new hosted service plus its
  Deployment manifest, health endpoint, and KEDA `ScaledObject`
  ([kubernetes-deployment-best-practices.instructions.md](kubernetes-deployment-best-practices.instructions.md)),
  matched to the RU-budget formula in
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md) §6.
* **Run the layered coverage gate** — check Domain/Application (85%) and
  Infrastructure (70%) coverage separately, per
  [engineering-standards.instructions.md](engineering-standards.instructions.md) §7,
  rather than reading one blended number that hides which layer is
  actually under-tested.

**Don't create a Skill for:**

* A task that's genuinely one-off (no recurrence expected).
* A task already fully specified by reading a single `docs/ai/*` file
  directly — a Skill that just says "read cosmos-db.instructions.md and
  follow it" adds an indirection layer with no value.
* A task better served by a slash command or a plain instruction addition
  to an existing `docs/ai/*.instructions.md` file — Skills are for
  *procedures*, not for standards or rules (those stay in the numbered
  instruction files this repo already has).

## Location and format

* `.claude/skills/<skill-name>/SKILL.md` — kebab-case, verb-first name
  describing the outcome (`add-inventory-event-type`,
  `add-consumer-workload`, `verify-coverage-gate`), not the mechanism
  (`update-cosmos-and-kafka-files`).
* YAML frontmatter: `name` (matches the directory name) and `description`
  (one line, specific enough that Claude Code picks the right skill
  without opening it — the same bar the `docs/ai/*.instructions.md`
  `description` fields already meet).

## Content rules

* **Reference, don't restate.** A Skill for scaffolding a Cosmos repository
  points to [cosmos-db.instructions.md](cosmos-db.instructions.md) for the
  ETag/concurrency/partition-key rules — it does not re-derive them inline.
  If a Skill and an instruction file disagree, the instruction file wins
  (same precedence rule as [../../CLAUDE.md](../../CLAUDE.md)); fix the
  Skill, not the other way around.
* **Terse and imperative**, matching the style already established across
  `docs/ai/*.instructions.md` — numbered steps, no filler, no restating
  what a cross-referenced file already says.
* **Name the files it touches.** A Skill for "add a new inventory event
  type" should enumerate the actual files/projects it creates or edits
  (`Domain/Aggregates/...`, `Infrastructure/Repositories/...`, the test
  projects) rather than describing the change abstractly.
* **State its own scope boundary** the same way every `docs/ai/*` file
  does — what this Skill does and, if relevant, what it deliberately
  leaves for the developer to decide (e.g., "this Skill scaffolds the
  structure; it does not choose your partition key for you — see
  [cosmos-db.instructions.md](cosmos-db.instructions.md) §4").

## Keeping Skills in sync

If a `docs/ai/*.instructions.md` file's section numbering or file name
changes (the kind of break this repo's own audits have caught before —
see [engineering-standards.instructions.md](engineering-standards.instructions.md)'s
history of exactly this class of bug), grep `.claude/skills/` for
references to it in the same change. A Skill with a stale `§N` pointer
fails silently the same way a stale cross-reference between two
instruction files does — treat it with the same care.
