# Changelog

All notable changes to Baran OS will be documented in this file.

This project uses [Semantic Versioning](https://semver.org/) with per-module Go tags
(`protocol/v0.1.0`, `core/v0.1.0`, `sdk/v0.1.0`).

## [v0.1.0] — 2026-03-15

First public release of Baran OS — a fully functional distributed agent runtime with
event routing, capability discovery, workflow orchestration, an agent SDK, and
documentation.

### Protocol (`protocol/`)

- Protobuf-defined `AgentEvent` envelope with typed payload routing by metadata
- Event types: agent lifecycle (`agent.register`, `agent.unregister`, `agent.heartbeat`,
  `agent.error`), routing (`event.direct`, `event.broadcast`, `event.capability`),
  discovery (`discovery.announce`, `discovery.query`, `discovery.response`),
  workflow (`workflow.start`, `workflow.step`, `workflow.step.result`,
  `workflow.complete`, `workflow.failed`, `workflow.state.query`,
  `workflow.state.response`)
- Dot-notation event type convention; UUID v7 for all identifiers
- `correlation_id` for request/response patterns

### Core (`core/`)

- **Event bus**: NATS JetStream-backed `EventBus` interface with publish, subscribe,
  and request/response support
- **Agent registry**: KV-backed agent registration with state machine
  (REGISTERED → ACTIVE → INACTIVE → UNREGISTERED)
- **Health monitor**: heartbeat-based liveness detection with configurable timeouts
- **Event router**: centralized routing — direct, broadcast, workflow-scoped, and
  capability-based delivery with precedence rules and `StreamRegistry`
- **Capability discovery**: registry, announcer, handler, and query protocol for
  agents to advertise and find capabilities at runtime
- **Workflow engine**: full orchestration lifecycle — start, step dispatch, step
  results, completion, and failure; per-workflow JetStream streams (`WF-{id}`),
  KV state with CAS, step timeouts, `previous_results` forwarding, state query
  via correlation, and best-effort agent death detection
- **Runtime binary** (`core/cmd/baran`): single binary that starts embedded NATS,
  router, workflow engine, health monitor, and HTTP health endpoint; configured
  via CLI flags and environment variables

### SDK (`sdk/`)

- Go package for building agents in <20 lines of code
- Full agent lifecycle: connect, register, announce capabilities, handle steps,
  publish results, graceful shutdown
- Step dispatch with handler registration by event type
- Health ping (automatic heartbeat)
- In-memory LRU idempotency cache
- No direct NATS dependency — communicates through `core.EventBus`

### Examples

- **Wildfire emergency management** (`examples/wildfire/`): three agents
  (risk-estimation, resource-allocation, evacuation-planning) demonstrating a
  complete multi-step workflow end-to-end

### Documentation

- Project README with vision, use cases, and getting started guide
- GitHub Pages documentation site (Docsify): architecture overview, installation,
  quickstart, SDK reference, event catalog
- Landing page with Tailwind CSS

### Bug Fixes

- Fix data race between `dispatchStep` and `Stop` in SDK
- Fix data races on `natsServer` and `healthAddr` fields in runtime
