# IIS-WMS Integrations

## Project Overview

This repository implements the Inventory Information System (IIS) integration platform.

### Responsibilities

- Consume inventory events from Kafka
- Process business workflows
- Persist data in Azure Cosmos DB
- Publish events to Azure Service Bus
- Expose ASP.NET Core REST APIs
- Deploy to Azure Kubernetes Service (AKS)

---

# Purpose

This file is a lightweight bootstrap for Claude Code.

**Do not load every instruction document by default.**

Instead:

1. Understand the user's request.
2. Determine which technologies are affected.
3. Load only the relevant instruction documents.
4. Implement the smallest correct solution.

This significantly reduces token usage while preserving engineering standards.

---

# Instruction Loading Strategy

## Always Load (Code Changes Only)

For implementation, refactoring, or code review tasks always load:

- [Engineering Standards](docs/ai/engineering-standards.instructions.md)

This file defines:

- Target Framework
- Approved package versions
- Security standards
- Logging standards
- Code quality expectations
- Cross-cutting engineering practices

For documentation-only, Git, Markdown, or repository navigation tasks, this document does **not** need to be loaded unless explicitly required.

---

# Load Additional Instructions Only When Needed

| Task                                             | Instruction File                                                                                                             |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| C# implementation                                | [docs/ai/csharp.instructions.md](docs/ai/csharp.instructions.md)                                                             |
| Clean Architecture / DDD / SOLID                 | [docs/ai/dotnet-architecture-good-practices.instructions.md](docs/ai/dotnet-architecture-good-practices.instructions.md)     |
| Design Patterns                                  | [docs/ai/oop-design-patterns.instructions.md](docs/ai/oop-design-patterns.instructions.md)                                   |
| ASP.NET Core / REST APIs                         | [docs/ai/aspnet-rest-apis.instructions.md](docs/ai/aspnet-rest-apis.instructions.md)                                         |
| Cosmos DB                                        | [docs/ai/cosmos-db.instructions.md](docs/ai/cosmos-db.instructions.md)                                                       |
| Kafka / Azure Service Bus / Blob Storage / Polly | [docs/ai/integration-resiliency.instructions.md](docs/ai/integration-resiliency.instructions.md)                             |
| Kubernetes / AKS                                 | [docs/ai/kubernetes-deployment-best-practices.instructions.md](docs/ai/kubernetes-deployment-best-practices.instructions.md) |
| Claude Skills                                    | [docs/ai/skills-generation.instructions.md](docs/ai/skills-generation.instructions.md)                                       |

Never load unrelated instruction files.

---

# Task Routing Examples

## Rename Variable

Load:

- Engineering Standards
- C#

---

## Implement Cosmos Repository

Load:

- Engineering Standards
- C#
- Architecture
- Cosmos DB

---

## Build REST Endpoint

Load:

- Engineering Standards
- C#
- ASP.NET Core

---

## Modify Kafka Consumer

Load:

- Engineering Standards
- Integration Resiliency

---

## Kubernetes Deployment

Load:

- Engineering Standards
- Kubernetes

---

## Architecture Review

Load:

- Engineering Standards
- Architecture
- Design Patterns

---

## Documentation Update

No instruction files are required unless repository standards are directly affected.

---

# Missing Instruction Files

If a required instruction file:

- cannot be found,
- cannot be opened,
- or cannot be read,

then:

- Stop.
- Report the missing file.
- Do not assume its contents.
- Do not silently continue.

---

# Rule Precedence

When multiple instruction documents apply:

## 1. Specific beats General

Technology-specific guidance overrides generic guidance.

Example:

Cosmos DB guidance overrides generic repository guidance.

---

## 2. Engineering Standards

`engineering-standards.instructions.md`

is authoritative for:

- Framework versions
- Package versions
- Security
- Logging
- Coverage thresholds
- Cross-cutting standards

---

## 3. Architecture

`dotnet-architecture-good-practices.instructions.md`

owns:

- Clean Architecture
- SOLID
- DDD
- Test naming conventions

---

## 4. Unresolved Conflicts

If guidance conflicts:

Interactive session:

- Ask for clarification.

Non-interactive execution:

- Implement only the non-conflicting portions.
- Add:

```text
TODO(ai): unresolved instruction conflict
```

Document the conflict in the final summary.

---

# Development Principles

Always:

- Follow existing architecture.
- Keep changes as small as possible.
- Preserve backward compatibility.
- Respect existing repository conventions.
- Prefer readability.
- Prefer maintainability.
- Prefer composition over inheritance.
- Use dependency injection.
- Use asynchronous APIs where appropriate.

Avoid:

- Unnecessary abstraction
- Premature optimization
- Large unrelated refactoring
- Hidden behavioral changes

---

# Approved Dependencies

Do not introduce new NuGet packages without approval.

Use only packages already approved by the repository.

If a new dependency would improve the solution:

- Explain why.
- Describe trade-offs.
- Wait for approval.

---

# Security

Never:

- Commit secrets
- Commit API keys
- Commit certificates
- Commit passwords
- Commit connection strings

Always use the project's approved secret management strategy.

---

# Testing

For implementation changes:

- Build affected projects.
- Execute relevant tests whenever possible.

If tests cannot be executed:

Clearly explain:

- Why
- Which tests were skipped

Never report tests as passing unless they were actually executed.

---

# Microsoft Guidance

Validate against Microsoft guidance only when changing:

- ASP.NET Core
- Azure SDKs
- Cosmos DB
- Azure Services
- Authentication
- Performance-sensitive code
- Security-sensitive code
- Framework usage

Skip unnecessary validation for:

- Comments
- Documentation
- Markdown
- Formatting
- Variable renames
- Minor refactoring

---

# Performance

Prefer:

- Async APIs
- CancellationToken
- ConfigureAwait only where repository standards require it
- Efficient LINQ
- Batch operations
- Streaming over buffering
- Minimal allocations

Avoid:

- Sync-over-async
- Blocking calls
- Duplicate database queries
- Multiple enumerations
- Unnecessary object creation

---

# AI Workflow

For every task:

1. Understand the request.
2. Determine affected technologies.
3. Load only required instruction documents.
4. Follow repository standards.
5. Implement the smallest correct solution.
6. Explain significant architectural decisions.
7. Avoid inventing repository conventions.

---

# Claude Skills

For recurring workflows prefer Claude Skills instead of embedding detailed instructions in prompts.

Examples:

- Cosmos Investigation
- Incident Investigation
- Kafka Troubleshooting
- Azure Service Bus Review
- REST API Review
- Architecture Review
- Pull Request Review
- Test Generation
- Root Cause Analysis

Refer to:

- [docs/ai/skills-generation.instructions.md](docs/ai/skills-generation.instructions.md)

---

# Response Guidelines

Responses should:

- Be concise.
- Avoid repeating repository instructions.
- Explain trade-offs only when relevant.
- Clearly state assumptions.
- Report limitations honestly.
- Distinguish facts from assumptions.

---

# Optimization Goal

This repository is optimized for Claude Code.

Minimize context usage by:

- Loading only required instruction files.
- Avoiding unnecessary reasoning.
- Avoiding duplicate instruction loading.
- Reusing Claude Skills for repetitive workflows.
- Keeping responses focused on the requested task.

Quality should never be sacrificed for brevity.
