# Changelog

All notable changes to Baran OS will be documented in this file.

This project uses [Semantic Versioning](https://semver.org/) with per-module Go tags
(`protocol/v0.1.0`, `core/v0.1.0`, `sdk/v0.1.0`).

## [v0.3.0] — 2026-03-22

### Core (`core/`)

- **EventStore**: Query historical events from existing JetStream streams by time range,
  event type, workflow ID, and source agent. Reads directly from JetStream using ordered
  consumers with `DeliverByStartTimePolicy` — no duplication of stored events.
- **ReplayEngine**: Replay completed workflows on an isolated SIMULATION stream. Supports
  configurable speed (real-time, accelerated, max speed), session state machine
  (PENDING → RUNNING → COMPLETED/STOPPED/ERROR), and in-memory session management.
- **Replay isolation**: Replayed events receive new UUID v7 IDs and metadata markers
  (`simulation.replay=true`, `simulation.session_id`, `simulation.original_timestamp`,
  `simulation.original_id`). Events are published exclusively to the SIMULATION stream —
  live agents and workflow streams are never affected.
- **Replay REST API**: Full lifecycle management via REST — create sessions
  (`POST /api/replay/sessions`), list/filter sessions (`GET /api/replay/sessions`),
  inspect sessions (`GET /api/replay/sessions/{id}`), stop sessions
  (`POST /api/replay/sessions/{id}/stop`).
- **Event query REST API**: `GET /api/events` with time range, event type, workflow ID,
  source agent filters, and pagination. `GET /api/events/workflows/{id}` for per-workflow
  event history.
- **SSE replay streaming**: `GET /api/replay/sessions/{id}/stream` delivers replay events
  in real time via Server-Sent Events (`replay.event`, `replay.complete`, `replay.stopped`,
  `replay.error`).

### Protocol (`protocol/`)

- New `simulation.proto` with `SimulationReplayStartPayload`, `SimulationReplayStopPayload`,
  `SimulationReplayCompletePayload` messages
- New `SIMULATION` stream (`simulation.>`, 24h retention)

### Examples

- Added replay demo instructions to wildfire example: query workflow events and replay
  a completed workflow using the REST API

### Documentation

- New [Event Replay & Simulation guide](docs/guide/simulation.md): EventStore, ReplayEngine,
  REST API reference, session lifecycle, replay isolation, and architecture notes
- Updated event catalog with simulation event category (3 new event types) and SIMULATION stream
- Updated README with event replay capability

---

## [v0.2.0] — 2026-03-21

### Core (`core/`)

- **Multi-node federation**: nodes discover each other via seed addresses using NATS leaf
  node connections. Each node maintains a `node-registry` KV bucket with peer health status.
  Node registration is propagated through the cluster with one-hop anti-loop protection.
- **Distributed capability sharing**: capabilities registered on any node are propagated to
  all federated peers via `federation.capability.announce` events. `FindByCapability` returns
  both local and remote agents transparently; local agents are always preferred for dispatch.
- **Cross-node event relay**: the router detects remote target agents and relays events via
  `FederationRelayPayload` to the target node. Workflow steps execute on remote agents without
  changes to the workflow engine or agent SDK.
- **Federation health monitoring**: `NodeMonitor` pings peers at configurable intervals,
  transitions nodes through `ACTIVE → UNHEALTHY → DEAD` states on missed heartbeats, and
  triggers automatic capability purge when a node is marked `DEAD`.
- **Dead node cleanup**: a background cycle removes `DEAD` nodes from the `node-registry`
  after a configurable `CleanupTTL`, keeping the registry lean in long-running clusters.
- **Federation REST API**: `GET /api/federation/nodes` returns all known nodes with status,
  address, capabilities count, and timestamps. `GET /healthz` includes a `federation` key.
- **Federation is opt-in**: nodes without `--federation-seeds` run in standalone mode with
  no behavioral change.

### Protocol (`protocol/`)

- New `federation.proto` with `NodeRegisterPayload`, `NodeUnregisterPayload`,
  `NodeHealthPingPayload`, `NodeHealthPongPayload`, `NodeStatus` enum,
  `FederationCapabilityPayload`, `FederationCapabilityRemovePayload`, `FederationRelayPayload`
- New `FEDERATION` stream (`federation.>`, 24h retention)
- `AgentRegistration` extended with `Origin` field (`"local"` / `"remote"`) and `IsRemote()` helper
- `StrategyRelay` added to routing strategy constants

### Examples

- Updated wildfire example README with multi-node federation setup: fire detection agents
  on Node A, evacuation planner on Node B, workflow executes transparently across nodes

### Documentation

- New [Federation guide](docs/guide/federation.md): overview, quickstart, configuration flags,
  node state machine, architecture diagram, REST API reference, and limitations

---

## [Unreleased]

### Core (`core/`)

- **Human-in-the-loop decisions**: workflow steps can require human approval via
  `human_approval: true` in step definitions. The Decision Coordinator pauses the
  workflow in `WAITING_HUMAN` status and publishes `human.decision.request` events.
  Operators approve or reject through the built-in web UI or REST API. Includes
  conflict detection when multiple decisions compete for the same resources.
- **Operator web UI**: embedded web dashboard at `/ui/` for reviewing and acting on
  pending decisions. Real-time updates via Server-Sent Events (SSE). REST API at
  `/api/decisions` for programmatic integration.
- **Decision recovery**: on startup, the coordinator scans for workflows stuck in
  `WAITING_HUMAN` and re-publishes their decision requests.
- **Workflow stream consolidation**: refactored per-workflow stream management for
  cleaner lifecycle and resource cleanup.

### Protocol (`protocol/`)

- New `human.proto` with `HumanDecisionRequestPayload`, `HumanDecisionResponsePayload`,
  `DecisionConflictPayload`, `DecisionResolvedPayload`, and `DecisionAction` enum
- New `WAITING_HUMAN` workflow status in `WorkflowStatus` enum
- Extended `StepDefinition` with `human_approval`, `prompt`, and `resource_ids` fields
- New streams: `HUMAN` (decision events) and `COORDINATION` (conflict events)

### Examples

- Updated wildfire example with a human approval step (4-step workflow)

---

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
