# AGENTS.md

## Purpose

This repository is a NATS platform evaluation and reference implementation.
It contains application code, NATS integrations, Docker Compose setups, CLI/SDK examples, designs, and documentation used to evaluate NATS capabilities and establish enterprise usage standards.

Treat every implementation as both realistic application code and a reusable NATS reference pattern. Prefer simple, explicit, idiomatic implementations over demo-only shortcuts or unnecessary architecture.

## Decision Principles

When multiple approaches are possible, prefer:

1. Correct NATS semantics
2. Simplest clear implementation
3. Idiomatic Go/React practices
4. Existing repository conventions
5. Operationally sensible behavior
6. Minimal dependencies
7. Easy extension
8. Abstraction only when justified

Do not optimize for theoretical scalability or sophistication at the expense of clarity.

## Agent Workflow

For every requested change:

1. Inspect relevant code, configuration, and documentation.
2. Understand existing APIs, subjects, streams, consumers, message flows, and conventions.
3. Create an implementation plan before modifying files.
4. Implement the smallest complete change.
5. Review affected code and related artifacts.
6. Validate within command restrictions.
7. Update documentation when behavior changes.
8. Update `docs/CHANGELOG.md` for meaningful changes.
9. Report changes, validation, manual commands, and limitations.

Do not make unrelated improvements. Mention broader issues separately unless required for the current change.

## Implementation Plan

Every code change requires a plan. For small changes, keep it concise. The plan must identify what changes, why, affected areas, approach, and important NATS or compatibility considerations.

## NATS Reference Patterns

Every NATS capability should make clear: capability, application use case, message flow, configuration, success behavior, failure behavior, and how the behavior is observed.

Implement NATS features to demonstrate meaningful capabilities or useful standard patterns, not merely for completeness.

### Subjects

Use clear, stable, hierarchical subject names. Document purpose, publisher, subscriber/consumer, message type, and stream/consumer relationship.

### Streams and Consumers

For JetStream, explicitly choose and document relevant semantics: retention, storage, replicas, acknowledgement, delivery/replay policy, durable/pull/push consumers, redelivery, max deliveries, backoff, error/dead-letter handling, and ordering.

Do not configure an option merely because it exists. Non-default behavior must have a reason.

### Messaging Semantics

Keep Core NATS, Request/Reply, Queue Groups, JetStream, Work Queues, durable consumers, pull/push consumers, KV, and Object Store semantics distinct. Do not hide important NATS behavior behind unnecessary abstractions.

## Application Code

Keep application code production-like but intentionally small. Prefer standard Go conventions, direct NATS client usage, explicit control flow, focused functions, clear models/handlers, useful error context, minimal dependencies, and environment-based configuration.

Avoid unnecessary generic frameworks, complex dependency injection, excessive interfaces, factory/repository patterns, generic messaging layers, configuration frameworks, or wrappers around the NATS client.

Introduce an abstraction only when it is an established standard, removes meaningful complexity, isolates a changing concern, is required by the design/capability, or materially improves testing.

## Code Quality

Code must be understandable to a developer unfamiliar with the repository. Prefer clear names, focused functions, explicit flow, and local reasoning over generic abstractions, deep indirection, or implicit behavior.

Remove stale, duplicated, redundant, or unused code when directly related to the change. Do not split code merely to reduce line count.

## Errors and Logging

Handle errors explicitly and preserve useful context. Do not silently ignore errors without reason.

Logs should expose meaningful NATS behavior such as connection, consumer start/stop, publish, request/reply, receive, processing, ACK/NACK, and redelivery. Avoid noise and unnecessary payload logging.

## Comments

Comments explain why, not obvious what. Use them for non-obvious NATS behavior, deliberate configuration choices, workarounds, retry/failure semantics, or important reference-pattern decisions.

## Configuration and Deployment

Keep runtime configuration simple and explicit; prefer environment variables. Docker Compose and deployment examples must be runnable, minimal, understandable, and consistent with the application. Document required NATS server configuration and non-obvious settings.

## CLI and SDK Examples

CLI and SDK examples are part of the NATS reference standard. Keep examples valid, copy/paste friendly, minimal, and consistent with the implementation. Explain important options and expected behavior without unnecessary variations or narrative.

## Frontend

The frontend is a demonstration UI. Use a React SPA with shallow components, minimal dependencies, straightforward state management, and simple API integration.

The frontend communicates with backend APIs and must not contain unnecessary business logic, understand NATS internals, receive NATS credentials, or connect directly to NATS.

Keep visual quality professional through clear hierarchy, spacing, typography, responsive layouts, useful states, and consistent status indicators. Do not sacrifice maintainability for visual effects or frontend architecture.

## Documentation

Documentation is part of implementation. Maintain:

* `docs/DEVELOPER_GUIDE.md`
* `docs/DEPLOYMENT_GUIDE.md`
* `docs/CHANGELOG.md`

Developer Guide: purpose/structure, service responsibilities, APIs, NATS capability mapping, important concepts, and common development changes.

Deployment Guide: runtime components, NATS/backend/frontend deployment, configuration, connectivity, topology, and verification.

Update documentation in the same change when APIs, subjects, streams, consumers, message models, service behavior, configuration, deployment, or demo flows change. Do not turn these guides into full architecture documents.

## Change Record

Record meaningful changes in `docs/CHANGELOG.md` with date, change, reason, and affected area. Do not record formatting-only or trivial edits.

## ASCII-Only

Use ASCII characters only in source code, comments, logs, errors, CLI output, configuration examples, test data, and documentation examples. Avoid Unicode arrows, smart quotes, emoji, and decorative symbols.

## Command Restrictions

The agent may inspect files, directories, source code, configuration, and documentation.

Do not execute Go, Git, npm/pnpm/yarn/bun, package-manager, dependency-management, or repository-operation commands. Examples include `go build`, `go test`, `go run`, `go mod tidy`, `git status`, `git diff`, `git commit`, `npm install`, `npm run`, and `npm test`.

If validation requires a restricted command, document it for the developer instead of executing it.

## Validation

After implementation, inspect affected code and verify imports/naming, error handling, API contracts, NATS subjects, message structures, stream/consumer configuration, runtime configuration, documentation consistency, backward compatibility, and unintended changes.

For NATS changes, ensure implementation, configuration, CLI/SDK examples, and documentation describe the same semantics.

If runtime validation is restricted, state exactly what the developer should run manually.

## Backward Compatibility

Before changing an API, subject, message model, stream, consumer, configuration, or demo flow:

1. Inspect existing usages.
2. Understand the impact.
3. Prefer backward-compatible changes when practical.
4. Update documentation and the changelog.

Do not silently break existing reference or demonstration flows. Document intentional breaking changes.

## Scope Discipline

Implement only what is required for the current capability or reference pattern. Do not silently change architecture, dependencies, APIs, folder structure, infrastructure, styling, naming conventions, or deployment unless required.

Do not implement future capabilities merely because they may be useful later.

## Final Standard

Every implementation must be treated as a **candidate enterprise NATS usage pattern**, not merely a working demo.

It should be:

* Correct in NATS semantics
* Representative of a sound enterprise usage pattern
* Explicit about configuration, behavior, and operational implications
* Simple enough to understand and adopt
* Observable and demonstrable
* Maintainable as application code
* Documented for future reuse
* Reusable as a reference implementation

The objective is to build **practical, technically sound NATS patterns that can inform enterprise architecture, development, integration, deployment, and operational standards**.

Do not optimize for "demo code that works." Optimize for **"reference code that demonstrates how NATS should be used by standards."**


Avoid over-engineering, unnecessary abstraction, redundant code, hidden NATS behavior, excessive configuration, and documentation noise.

The objective is to build technically sound NATS usage patterns that enterprise developers can understand, evaluate, and reuse.
