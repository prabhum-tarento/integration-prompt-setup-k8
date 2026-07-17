---
description: 'C# language idioms, naming, and formatting for this codebase. Language-level only — architecture, API, and data-access rules live in the sibling docs listed in CLAUDE.md.'
applyTo: '**/*.cs'
---

# C# Language Guidelines

This file owns **C# language idioms and formatting only**. It does not
cover architecture (see
[dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md)),
API design (see [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md)),
or data access (see [cosmos-db.instructions.md](cosmos-db.instructions.md)).
If you're about to add a section on auth, logging, or deployment here, it
belongs in one of those files instead — don't duplicate it.

## Language version

* Target C# 14 on .NET 10 (see
  [engineering-standards.instructions.md](engineering-standards.instructions.md) §1
  for the canonical version — if this line and that file ever disagree, that
  file wins).

## Async

* Every asynchronous method name ends with `Async` and returns
  `Task`/`Task<T>`/`ValueTask<T>` — never `async void` except a top-level
  event handler.
* Forward the ambient `CancellationToken` to every awaited call that accepts
  one. Don't swallow it by calling an overload without it.
* Never call `.Result`, `.Wait()`, or `.GetAwaiter().GetResult()` on a task
  from non-test code — it risks deadlocks in ASP.NET Core's synchronization
  context-free environment and defeats the point of going async.
* Wrap `IDisposable`/`IAsyncDisposable` resources in `using`/`await using`
  declarations; don't rely on finalizers.
* For fan-out work bounded by an external resource's capacity (e.g., Cosmos
  RU budget, downstream API rate limits), bound concurrency explicitly with
  `SemaphoreSlim` or `Parallel.ForEachAsync`'s `MaxDegreeOfParallelism` —
  see [integration-resiliency.instructions.md](integration-resiliency.instructions.md)
  for the pattern used in the message-processing pipeline. Don't fire
  unbounded `Task.WhenAll` over a collection whose size isn't known to be
  small.

## Exceptions

* Throw specific exception types (custom domain exceptions from the Domain
  layer, or a documented framework exception) — never a bare
  `throw new Exception(...)`.
* Catch only exceptions you can meaningfully handle, log, or translate.
  Don't catch-and-swallow; don't catch `Exception` at a boundary that isn't
  the global exception handler (see
  [aspnet-rest-apis.instructions.md](aspnet-rest-apis.instructions.md)).
* Validate inputs and fail fast at the boundary (Api request binding,
  message deserialization) rather than deep inside a call chain.

## Method design

* Aim for 10–20 lines of executable code per method; treat 40+ lines as a
  signal to split it, unless there's a clear technical reason not to (a
  large switch expression, generated code, a performance-critical
  algorithm). Line count is a signal, not the goal — prioritize reducing
  cognitive complexity over hitting a specific number.
* Prefer three or fewer parameters. Beyond that, introduce a request/options
  object or a domain model rather than adding a fourth parameter.
* Minimize nesting with guard clauses and early returns; three nested levels
  is the practical ceiling. Avoid an `else` block after a `return`.
* Keep every statement in a method at one level of abstraction — a
  high-level orchestration method delegates implementation detail to
  well-named helpers rather than mixing business rules with low-level
  detail inline.
* Single Responsibility at the method level follows the same SRP
  definition [dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md)
  uses at the class level — this file doesn't restate it. A method that
  validates, mutates state, and logs has more than one reason to change;
  split it.

## Naming conventions

* PascalCase for type names, method names, and public members.
* camelCase for private fields and local variables, **no `_` or `s_`
  prefix** — this is the fixed default for this repo, not "whatever the
  nearest file does." (e.g. `private readonly IInventoryEventRepository repository;`,
  set via a primary constructor parameter where the type allows it, not a
  separately declared backing field.)
* Prefix interfaces with `I` (e.g., `IInventoryRepository`).

## Formatting

* Apply the formatting defined in `.editorconfig` — don't hand-format
  against it. If `.editorconfig` doesn't exist yet at the repo root (this
  is a greenfield repo — check before assuming it's there), create it as
  part of whichever change first needs it: start from .NET's
  `dotnet new editorconfig` baseline and layer the Allman-brace and
  file-scoped-namespace choices below on top, rather than formatting that
  first file by eye and leaving the config to a later PR.
* File-scoped namespaces and single-line `using` directives.
* Newline before the opening brace of any block (`if`, `for`, `while`,
  `foreach`, `using`, `try`, etc.), matching the existing `.editorconfig`
  Allman-style setting.
* The final `return` of a method is on its own line.
* Use `nameof` instead of string literals when referring to a member name.
* Use pattern matching and switch expressions when they read more clearly
  than an `if`/`else if` chain — not as an absolute rule. A single boolean
  check (`if (x is null)`) does not need to become a switch expression.
* **XML doc comments are required on every class, record, and method in
  `src/`** (Domain, Application, Infrastructure, Api) — at minimum a
  `<summary>` stating what the type or member does, plus `<param>`/
  `<returns>` where the name and type don't already make that obvious.
  `Application`'s public use-case interfaces (e.g. `IInventoryEventService`),
  `Domain`'s public aggregate/value-object methods, and `Infrastructure`
  repository interfaces (e.g. `IInventoryEventRepository`, per
  [cosmos-db.instructions.md](cosmos-db.instructions.md) §5) get the richest
  docs — include `<example>`/`<code>` where the usage isn't obvious from the
  signature — since those are the types another layer or another engineer
  reads without opening the implementation. A trivial DTO or a one-line
  property still gets a one-line `<summary>`; it does not need `<example>`,
  `<remarks>`, or documented parameters it doesn't have.
  Test projects are the one exception, at the method level only: a
  `[Fact(DisplayName = "...")]` already states what the test verifies, so
  don't add a redundant `<summary>` above it. A test *class* still gets one,
  describing the subject/scenario it covers.
* Inline `//` comments belong on non-obvious logic — a branch whose
  reasoning isn't apparent from the code around it, a workaround for a
  specific bug or SDK quirk, an invariant a future edit could silently
  break. Don't comment a line whose meaning the code already fully carries
  (`var count = items.Count;` does not need `// get the count`) — that adds
  noise, not documentation, and works against this file's readability
  goals.

## Nullable reference types

* Declare variables non-nullable by default; check for `null` only at entry
  points (API model binding, message deserialization, external SDK
  returns).
* Use `is null` / `is not null`, not `== null` / `!= null`.
* Trust the compiler's nullable analysis — don't add a null check the type
  system already guarantees is unreachable.

## Testing

* Test method names follow `MethodName_Condition_ExpectedResult()` — this
  convention is owned by
  [dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md) §4;
  this file doesn't define an alternative.
* Do not emit `// Arrange` / `// Act` / `// Assert` comments — structure the
  test body in that order without labeling it.
* Mock interfaces at the Application/Infrastructure boundary; don't mock
  concrete classes.
