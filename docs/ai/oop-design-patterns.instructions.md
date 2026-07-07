---
description: 'Gang of Four design pattern selection for this C# codebase. SOLID definitions and doc-comment style are owned by sibling docs — this file references them rather than restating them.'
applyTo: '**/*.cs'
---

# Design Patterns for This Codebase

This file guides *when* to reach for a GoF pattern in C#. It does not define
SOLID (canonical text:
[dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md) §1)
or doc-comment style (canonical text:
[csharp.instructions.md](csharp.instructions.md)) — apply those, don't
restate them here.

## Most relevant for this service

Of the patterns catalogued below, **Strategy, Decorator, Chain of
Responsibility, Factory Method, Adapter, and Command** are the ones likely
to actually appear in a Kafka → Service Bus → Cosmos DB integration
pipeline (pluggable allocation policies, message-processing middleware,
retry/fallback chains, per-warehouse strategy selection). The remainder
(Visitor, Memento, Flyweight, Bridge, Composite, Prototype, Iterator) are
kept for completeness and for the rare case they genuinely fit, but don't
reach for one of them just because it's on the list — see "Apply a pattern
only when it solves a real, present problem" below.

**Repository is not GoF-catalogued here on purpose.** It's not one of the
23 classic patterns, and it's already fully specified for this codebase in
[cosmos-db.instructions.md](cosmos-db.instructions.md) §5 — don't invent a
second repository abstraction from this file's "program to an interface"
philosophy; use the one that already exists.

## Core Architectural Philosophy

- **Program to an interface, not an implementation**: favor abstract
  classes/interfaces over concrete types; use DI to supply concrete
  instances.
- **Favor composition over inheritance**: combine behaviors dynamically at
  runtime; avoid deep inheritance trees; use delegation to reuse behavior
  without breaking encapsulation.
- **Encapsulate what varies**: separate the parts of the application that
  change from the parts that don't, using Strategy, State, or Bridge to
  isolate the variation.
- **Loose coupling**: minimize direct dependencies; use Mediator, Observer,
  or an abstract factory to keep components decoupled.

## Creational Patterns

- **Abstract Factory**: a system must be configured with one of several
  families of related products; clients interact only with the abstract
  factory and abstract product interfaces.
- **Factory Method**: a class can't anticipate the concrete type it must
  create; defer instantiation to a subclass or a factory delegate.
- **Builder**: constructing a complex object requires a step-by-step
  process, especially when the same steps can produce different results.
- **Singleton**: use only when a true single instance is required (e.g., the
  `CosmosClient` registration in
  [cosmos-db.instructions.md](cosmos-db.instructions.md)). In an ASP.NET
  Core app, prefer registering the type as a DI **singleton service**
  instead of a static `Instance` — this keeps it testable and avoids the
  classic bug where a hand-rolled Singleton captures a scoped/transient
  dependency and leaks it across requests. Never make a Singleton depend on
  a Scoped service.
- **Prototype**: cloning an existing object is cheaper than building one
  from scratch, or you want to avoid a factory-class hierarchy.

## Structural Patterns

- **Adapter**: make incompatible interfaces work together; prefer object
  adapters (composition) over class adapters.
- **Bridge**: separate an abstraction from its implementation so both can
  vary independently.
- **Composite**: represent part-whole hierarchies; clients treat individual
  objects and compositions uniformly via a common interface.
- **Decorator**: attach responsibilities to an object dynamically; prefer
  this over subclassing to avoid class explosion; the decorator must expose
  the exact interface of the thing it decorates.
- **Facade**: provide a simple, unified interface to a complex subsystem.
- **Flyweight**: minimize memory/compute by sharing state across similar
  objects.
- **Proxy**: a surrogate that controls access to another object (lazy
  loading, access control, remote calls).

## Behavioral Patterns

- **Strategy**: a family of interchangeable algorithms; use it to eliminate
  a `switch`/`if-else` chain that selects behavior.
- **Observer**: one-to-many notification between a subject and its
  observers, kept loosely coupled. **Domain events** (`StockReserved`,
  `StockAllocated`, … per
  [dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md) §1)
  are this codebase's concrete use of the Observer idea, but they are
  dispatched through **MediatR notifications**
  (`INotification`/`INotificationHandler<T>`), not a hand-rolled C# `event`
  or a custom pub/sub type. Raise the event by adding it to the aggregate's
  domain-event collection and publishing via `IMediator.Publish` from the
  Application layer after the aggregate's changes are persisted — don't
  invoke a raw `event` delegate from inside the aggregate itself, which
  would let a handler run mid-transaction with a partially-applied state
  change.
- **Command**: encapsulate a request as an object — useful for undo/redo,
  queues, or logging requests.
- **State**: an object's behavior depends on its internal state and must
  change at runtime; represent each state as its own class.
- **Template Method**: define an algorithm's skeleton in a base class,
  deferring specific steps to subclasses.
- **Chain of Responsibility**: pass a request along a chain of handlers
  until one handles it, without coupling the sender to a specific receiver.
- **Mediator**: centralize communication between a set of objects so they
  don't reference each other directly.
- **Iterator**: a standard way to traverse an aggregate without exposing its
  internal representation.
- **Visitor**: define a new operation over an object structure without
  changing the element classes — effective for stable composite structures.
- **Memento**: capture and restore an object's internal state without
  violating encapsulation (undo mechanisms).

## Applying patterns in this codebase

**Design principles**

- Generate the interface or abstract base type before its concrete
  implementations.
- Fields are `private` by default; expose getters/setters only when needed;
  favor immutable types (records, `init`-only properties).
- Name types after the pattern when it aids understanding
  (`TaxCalculationStrategy`, `InventoryEventDecorator`), but keep names
  domain-natural when a pattern name would obscure intent.
- Break up large classes into smaller, focused ones coordinated by a
  Mediator or composed of Strategy objects rather than growing one God
  class.
- Apply a pattern only when it solves a real, present problem — don't
  introduce Strategy/Factory/Observer speculatively for a single
  implementation with no near-term second one.

**Testability**

- Choose patterns that keep code testable (DI for easy mocking); write
  tests that verify the pattern's behavior, not just its structure.
- When refactoring existing code toward a pattern, do it in small,
  test-covered increments rather than one large rewrite.

**Performance**

- Patterns add indirection; profile before adding one purely for
  "flexibility" in a hot path (e.g., the message-processing pipeline in
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md)).

## Logging and error handling

- Integrate logging and error handling into whatever pattern you apply —
  fail loud, clear, and early; never swallow an exception silently.
- Use custom domain exceptions for meaningful, granular error handling (see
  [dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md)).
- Use `try`/`catch` for expected error conditions, not for normal control
  flow.
- Use the appropriate log level (`Debug`/`Information`/`Warning`/`Error`/`Critical`)
  — see [engineering-standards.instructions.md](engineering-standards.instructions.md) §6
  for the structured-logging and correlation ID requirements every log
  statement must satisfy.

## Documentation

- Doc-comment style (XML doc comments, when they're required) is defined in
  [csharp.instructions.md](csharp.instructions.md) — this file doesn't
  define its own convention.
- When you apply a named pattern, add a one-line comment stating the
  pattern and why (e.g., `// Strategy: pluggable allocation policies per warehouse`).
- Don't create a new documentation file for a pattern explanation — extend
  the existing architecture doc or a code comment. Avoid redundant or
  overly verbose documentation; keep it concise and focused on what a
  future maintainer actually needs.
