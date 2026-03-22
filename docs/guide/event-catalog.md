# Event Catalog

Baran OS uses 22 event types across 7 categories. All events are wrapped in an `AgentEvent` envelope — the envelope routes, the payload describes.

## Overview

| Category | Event Types | Count |
|----------|------------|-------|
| [System](#system) | `agent.register`, `agent.unregister`, `agent.error` | 3 |
| [Health](#health) | `agent.health.ping`, `agent.health.pong` | 2 |
| [Discovery](#discovery) | `agent.capability.announce`, `agent.discovery.request`, `agent.discovery.response` | 3 |
| [Workflow](#workflow) | `workflow.start`, `workflow.step`, `workflow.step.result`, `workflow.complete`, `workflow.failed` | 5 |
| [Workflow Query](#workflow-query) | `workflow.state.request`, `workflow.state.response` | 2 |
| [Human Decision](#human-decision) | `human.decision.request`, `human.decision.response`, `decision.conflict`, `decision.resolved` | 4 |
| [Simulation](#simulation) | `simulation.replay.start`, `simulation.replay.stop`, `simulation.replay.complete` | 3 |

## Event Envelope

Every event is wrapped in an `AgentEvent` message:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | UUID v7, unique per event |
| `type` | string | Dot-notation event type |
| `source_node` | string | Origin node ID |
| `source_agent` | string | Origin agent ID |
| `target_agent` | string | Destination agent ID (optional) |
| `workflow_id` | string | Workflow UUID (optional) |
| `correlation_id` | string | Request/response correlation (optional) |
| `timestamp` | int64 | Unix nanoseconds |
| `metadata` | map | Routing hints (e.g., `route.capability`) |
| `payload` | bytes | Serialized typed protobuf |

---

## System

System events manage the agent lifecycle.

### `agent.register`

Emitted when an agent connects and announces itself to the runtime.

- **Emitted by**: Agent (via SDK)
- **Consumed by**: Registry, Discovery Announcer
- **Stream**: AGENTS

**Payload: `AgentRegisterPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `agent_id` | string | UUID v7 |
| `agent_type` | string | Agent type classification |
| `version` | string | Semantic version |
| `capabilities` | repeated Capability | List of capabilities |
| `labels` | map | Optional metadata |

**Capability fields:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Capability identifier |
| `version` | string | Semantic version |
| `description` | string | Human-readable description |
| `parameters` | map | Optional key-value metadata |

### `agent.unregister`

Emitted when an agent shuts down gracefully.

- **Emitted by**: Agent (via SDK)
- **Consumed by**: Registry
- **Stream**: AGENTS

**Payload: `AgentUnregisterPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `agent_id` | string | UUID v7 |
| `reason` | string | Reason for unregistering (e.g., "shutdown") |

### `agent.error`

Emitted when the runtime detects an agent-level error (e.g., agent declared dead).

- **Emitted by**: Health Monitor, Router
- **Consumed by**: Logging, Monitoring
- **Stream**: AGENTS

**Payload: `AgentErrorPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `agent_id` | string | UUID v7 |
| `error_code` | string | Error code (e.g., `ROUTER_TARGET_NOT_FOUND`, `ROUTER_NO_CAPABILITY_MATCH`) |
| `message` | string | Human-readable error message |
| `stack_trace` | string | Optional stack trace |
| `workflow_id` | string | Optional associated workflow |

---

## Health

Health events implement the heartbeat protocol between the runtime and agents.

### `agent.health.ping`

Sent by the runtime to check if an agent is alive.

- **Emitted by**: Health Monitor
- **Consumed by**: Agent (via SDK, automatic response)
- **Stream**: HEALTH

**Payload: `HealthPingPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `agent_id` | string | Target agent UUID v7 |
| `sequence` | int64 | Monotonically increasing sequence number |

### `agent.health.pong`

Response from an agent confirming it is alive.

- **Emitted by**: Agent (via SDK, automatic)
- **Consumed by**: Health Monitor
- **Stream**: HEALTH

**Payload: `HealthPongPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `agent_id` | string | Responding agent UUID v7 |
| `sequence` | int64 | Echo of ping sequence |
| `status` | AgentStatus | `HEALTHY`, `DEGRADED`, or `OVERLOADED` |
| `resources` | ResourceUsage | Optional CPU/memory metrics |

**AgentStatus values:** `AGENT_STATUS_HEALTHY`, `AGENT_STATUS_DEGRADED`, `AGENT_STATUS_OVERLOADED`

---

## Discovery

Discovery events enable agents to find each other by capability.

### `agent.capability.announce`

Published when an agent registers or unregisters, broadcasting its capabilities to the network.

- **Emitted by**: Discovery Announcer
- **Consumed by**: Any interested subscriber
- **Stream**: DISCOVERY

**Payload: `CapabilityAnnouncePayload`**

| Field | Type | Description |
|-------|------|-------------|
| `agent_id` | string | UUID v7 |
| `capabilities` | repeated Capability | Current capabilities (empty list = agent removed) |

### `agent.discovery.request`

Published to query for agents with a specific capability.

- **Emitted by**: Any component
- **Consumed by**: Discovery Handler
- **Stream**: DISCOVERY

**Payload: `DiscoveryRequestPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `capability_name` | string | Required capability name |
| `version_constraint` | string | Optional version prefix match |

### `agent.discovery.response`

Response to a discovery request with matching agents.

- **Emitted by**: Discovery Handler
- **Consumed by**: Requesting component (matched via `correlation_id`)
- **Stream**: DISCOVERY

**Payload: `DiscoveryResponsePayload`**

| Field | Type | Description |
|-------|------|-------------|
| `matches` | repeated AgentCapabilityMatch | List of matching agents |

**AgentCapabilityMatch fields:**

| Field | Type | Description |
|-------|------|-------------|
| `agent_id` | string | UUID v7 |
| `agent_type` | string | Agent type |
| `capabilities` | repeated Capability | Matched capabilities |

---

## Workflow

Workflow events manage multi-step workflow execution.

### `workflow.start`

Triggers a new workflow.

- **Emitted by**: Trigger program or any component
- **Consumed by**: Workflow Engine
- **Stream**: AGENTS

**Payload: `WorkflowStartPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `definition` | WorkflowDefinition | Workflow definition |

**WorkflowDefinition fields:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Workflow name |
| `steps` | repeated StepDefinition | Ordered list of steps (1-100) |
| `initiator` | string | Agent ID or "runtime" |

**StepDefinition fields:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Step name |
| `capability` | string | Required capability |
| `timeout_seconds` | uint32 | Step timeout (0 = default 30s) |
| `input` | bytes | Domain-typed protobuf input |

### `workflow.step`

Dispatched to an agent to execute a workflow step.

- **Emitted by**: Workflow Engine
- **Consumed by**: Target Agent (via SDK)
- **Stream**: WF-{id} + DIRECT

**Payload: `WorkflowStepPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `step_index` | uint32 | 0-based step position |
| `step` | StepDefinition | Step definition |
| `workflow_id` | string | Parent workflow UUID |
| `previous_results` | repeated StepResult | Results from prior steps |
| `input` | bytes | Step input data |

### `workflow.step.result`

Published by an agent after completing a workflow step.

- **Emitted by**: Agent (via SDK)
- **Consumed by**: Workflow Engine
- **Stream**: WF-{id}

**Payload: `WorkflowStepResultPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `step_index` | uint32 | Which step was completed |
| `status` | StepStatus | `SUCCESS` or `FAILURE` |
| `result` | bytes | Serialized step result |
| `error` | WorkflowError | Error details (if FAILURE) |

### `workflow.complete`

Published when all workflow steps have completed successfully.

- **Emitted by**: Workflow Engine
- **Consumed by**: Trigger program, monitoring
- **Stream**: WF-{id}

**Payload: `WorkflowCompletePayload`**

| Field | Type | Description |
|-------|------|-------------|
| `workflow_id` | string | Workflow UUID |
| `results` | repeated StepResult | All step results |
| `started_at` | int64 | Workflow start time (Unix nanos) |
| `completed_at` | int64 | Completion time (Unix nanos) |

### `workflow.failed`

Published when a workflow fails due to a step failure or timeout.

- **Emitted by**: Workflow Engine
- **Consumed by**: Trigger program, monitoring
- **Stream**: WF-{id}

**Payload: `WorkflowFailedPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `workflow_id` | string | Workflow UUID |
| `error` | WorkflowError | Error details |
| `failed_step` | uint32 | Which step failed |
| `started_at` | int64 | Workflow start time (Unix nanos) |
| `failed_at` | int64 | Failure time (Unix nanos) |

**WorkflowError fields:**

| Field | Type | Description |
|-------|------|-------------|
| `code` | string | `STEP_TIMEOUT`, `AGENT_UNAVAILABLE`, `STEP_FAILED`, `INVALID_DEFINITION` |
| `message` | string | Human-readable error message |
| `step_index` | uint32 | Which step caused the error |
| `agent_id` | string | Agent involved (if applicable) |

---

## Workflow Query

Query events for inspecting workflow state.

### `workflow.state.request`

Request the current state of a workflow.

- **Emitted by**: Any component
- **Consumed by**: Workflow Engine
- **Stream**: AGENTS

**Payload: `WorkflowStateRequestPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `workflow_id` | string | Workflow UUID to query |

### `workflow.state.response`

Response with the current state of a workflow.

- **Emitted by**: Workflow Engine
- **Consumed by**: Requesting component (matched via `correlation_id`)
- **Stream**: AGENTS

**Payload: `WorkflowStateResponsePayload`**

| Field | Type | Description |
|-------|------|-------------|
| `workflow_id` | string | Workflow UUID |
| `status` | WorkflowStatus | `CREATED`, `RUNNING`, `COMPLETED`, `FAILED`, `WAITING_HUMAN` |
| `definition` | WorkflowDefinition | Original workflow definition |
| `current_step` | uint32 | Current step index |
| `step_results` | repeated StepResult | Results so far |
| `assigned_agent` | string | Agent assigned to current step |
| `error` | WorkflowError | Error details (if FAILED) |
| `created_at` | int64 | Creation time (Unix nanos) |
| `updated_at` | int64 | Last update time (Unix nanos) |

---

## Human Decision

Human decision events implement the human-in-the-loop approval protocol. When a workflow step has `human_approval: true`, the workflow pauses in `WAITING_HUMAN` status until an operator approves or rejects.

### `human.decision.request`

Published when a workflow reaches a step that requires human approval.

- **Emitted by**: Decision Coordinator
- **Consumed by**: Operator UI, external integrations
- **Stream**: HUMAN

**Payload: `HumanDecisionRequestPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `decision_id` | string | UUID v7, unique per decision |
| `workflow_id` | string | Parent workflow UUID |
| `step_index` | uint32 | 0-based step position |
| `step_name` | string | Human-readable step name |
| `prompt` | string | Decision question for the operator |
| `input` | bytes | Original workflow input (serialized) |
| `previous_results` | repeated StepResult | Results from prior steps |
| `resource_ids` | repeated string | Resource identifiers for conflict detection |
| `conflict_ids` | repeated string | IDs of other decisions that conflict with this one |

### `human.decision.response`

Published when an operator approves or rejects a decision.

- **Emitted by**: Operator UI or external integration
- **Consumed by**: Decision Coordinator → Workflow Engine
- **Stream**: Per-workflow (WF-{id}) + HUMAN

**Payload: `HumanDecisionResponsePayload`**

| Field | Type | Description |
|-------|------|-------------|
| `decision_id` | string | Which decision this responds to |
| `workflow_id` | string | Parent workflow UUID |
| `action` | DecisionAction | `APPROVE` or `REJECT` |
| `operator_id` | string | Who made the decision |
| `comment` | string | Optional reason or notes |
| `responded_at` | int64 | Unix nanoseconds |

**DecisionAction values:** `DECISION_ACTION_APPROVE`, `DECISION_ACTION_REJECT`

### `decision.conflict`

Published when two or more pending decisions compete for the same resources.

- **Emitted by**: Decision Coordinator
- **Consumed by**: Operator UI, monitoring
- **Stream**: COORDINATION

**Payload: `DecisionConflictPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `conflict_group_id` | string | UUID v7 grouping conflicting decisions |
| `decision_ids` | repeated string | All decision IDs in the conflict |
| `workflow_ids` | repeated string | Corresponding workflow IDs |
| `resource_ids` | repeated string | Overlapping resource identifiers |
| `detected_at` | int64 | Unix nanoseconds |

### `decision.resolved`

Published when a decision is approved or rejected, notifying related decisions in the same conflict group.

- **Emitted by**: Decision Coordinator
- **Consumed by**: Operator UI, monitoring
- **Stream**: COORDINATION

**Payload: `DecisionResolvedPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `decision_id` | string | Which decision was resolved |
| `workflow_id` | string | Parent workflow UUID |
| `action` | DecisionAction | `APPROVE` or `REJECT` |
| `conflict_group_id` | string | Conflict group (empty if none) |
| `related_decision_ids` | repeated string | Other decisions in the same conflict group |
| `resolved_at` | int64 | Unix nanoseconds |

---

## Simulation

Simulation events coordinate replay sessions on the isolated SIMULATION stream. See the [Event Replay & Simulation](simulation.md) guide for full details.

### `simulation.replay.start`

Published when a replay session begins executing.

- **Emitted by**: ReplayEngine
- **Consumed by**: SSE stream clients, monitoring
- **Stream**: SIMULATION

**Payload: `SimulationReplayStartPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `session_id` | string | UUID v7 of the replay session |
| `workflow_id` | string | Workflow being replayed |
| `speed` | double | Playback speed (0 = max) |
| `total_events` | int32 | Total events to replay |

### `simulation.replay.stop`

Published when a replay session is stopped by operator request or due to an error.

- **Emitted by**: ReplayEngine
- **Consumed by**: SSE stream clients, monitoring
- **Stream**: SIMULATION

**Payload: `SimulationReplayStopPayload`**

| Field | Type | Description |
|-------|------|-------------|
| `session_id` | string | UUID v7 of the replay session |
| `reason` | string | `"operator_request"` or `"error"` |
| `replayed_events` | int32 | Number of events replayed before stop |

### `simulation.replay.complete`

Published when all events in a replay session have been replayed.

- **Emitted by**: ReplayEngine
- **Consumed by**: SSE stream clients, monitoring
- **Stream**: SIMULATION

**Payload: `SimulationReplayCompletePayload`**

| Field | Type | Description |
|-------|------|-------------|
| `session_id` | string | UUID v7 of the replay session |
| `total_events` | int32 | Total events replayed |
| `duration_ms` | int64 | Replay duration in milliseconds |

---

## Stream Routing Summary

| Stream | Subjects | Max Age | Events |
|--------|----------|---------|--------|
| AGENTS | `agent.register`, `agent.unregister`, `agent.error`, `workflow.start`, `workflow.state.*` | 24h | System lifecycle + workflow triggers/queries |
| HEALTH | `agent.health.ping`, `agent.health.pong` | 1h | Heartbeat protocol |
| DISCOVERY | `agent.capability.announce`, `agent.discovery.*` | 24h | Capability announcements and queries |
| DIRECT | `agent.direct.>` | 24h | Targeted agent delivery |
| HUMAN | `human.decision.request`, `human.decision.response` | 24h | Human decision requests and responses |
| COORDINATION | `decision.conflict`, `decision.resolved` | 24h | Cross-workflow conflict detection and resolution |
| SIMULATION | `simulation.>` | 24h | Replay events and coordination |
| WF-{id} | `workflow.{id}.>` | 24h | Per-workflow events (created on demand) |
