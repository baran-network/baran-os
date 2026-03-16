# Architecture

Baran OS is an event-driven runtime that coordinates autonomous agents through typed events and multi-step workflows. This page describes each component and how they relate.

## Overview

```
┌───────────────────────────────────────────────────┐
│                   Baran Runtime                    │
│                                                    │
│  ┌──────────┐  ┌───────────┐  ┌────────────────┐  │
│  │  Router  │  │ Workflow  │  │  Capability    │  │
│  │          │  │  Engine   │  │  Discovery     │  │
│  └────┬─────┘  └─────┬─────┘  └───────┬────────┘  │
│       │               │               │            │
│  ┌────┴───────────────┴───────────────┴─────────┐  │
│  │           Event Bus (NATS JetStream)          │  │
│  └────┬───────────────┬───────────────┬─────────┘  │
│       │               │               │            │
└───────┼───────────────┼───────────────┼────────────┘
        │               │               │
  ┌─────┴─────┐  ┌──────┴──────┐  ┌─────┴──────┐
  │  Agent A  │  │   Agent B   │  │  Agent C   │
  │  (AI/LLM) │  │ (heuristic) │  │  (sensor)  │
  └───────────┘  └─────────────┘  └────────────┘
```

Agents are external processes that connect to the runtime over NATS. They register their capabilities, receive workflow steps matched to those capabilities, and publish results. The runtime handles everything else: routing, sequencing, state, health monitoring, and failure detection.

## Components

### Event Bus

The Event Bus is the communication backbone. All events flow through it — agents never communicate directly.

Built on [NATS JetStream](https://nats.io), it provides:

- **Publish/Subscribe** — any component can publish events and subscribe to event types
- **Persistence** — JetStream stores events in named streams with configurable retention
- **At-least-once delivery** — events are persisted and redelivered if not acknowledged

The Event Bus exposes a simple interface:

```go
type EventBus interface {
    Publish(ctx context.Context, event *Event) error
    Subscribe(ctx context.Context, eventType string, handler EventHandler) (Subscription, error)
    Close() error
}
```

Every event is wrapped in an `AgentEvent` envelope:

| Field | Purpose |
|-------|---------|
| `id` | UUID v7, unique per event |
| `type` | Dot-notation event type (e.g., `agent.register`) |
| `source_agent` | Origin agent ID |
| `target_agent` | Destination agent ID (optional) |
| `workflow_id` | Workflow UUID (optional) |
| `correlation_id` | Request/response correlation (optional) |
| `metadata` | Routing hints (e.g., `route.capability`) |
| `payload` | Serialized typed protobuf (opaque to the router) |

The envelope routes, the payload describes. The runtime never interprets payload contents — it routes based on envelope fields only.

### Router

The Router decides where events go. It inspects the event envelope and applies routing strategies in order of precedence:

1. **Direct** — if `target_agent` is set, the event goes to that specific agent via `agent.direct.{agentID}.{eventType}`. Used for workflow step dispatch and targeted responses.

2. **Capability-based** — if `metadata["route.capability"]` is set, the router queries the registry for agents with that capability and fans out the event to all matching agents.

3. **Workflow-scoped** — if `workflow_id` is set, the event goes to a per-workflow stream `WF-{workflowID}`. This keeps all events for a workflow ordered together.

4. **Broadcast** — all other events go to their mapped system stream (AGENTS, HEALTH, DISCOVERY).

#### Stream Mapping

| Stream | Event Types | Max Age | Purpose |
|--------|------------|---------|---------|
| AGENTS | `agent.register`, `agent.unregister`, `agent.error`, `workflow.start` | 24h | Agent lifecycle and workflow triggers |
| HEALTH | `agent.health.ping`, `agent.health.pong` | 1h | Health monitoring heartbeats |
| DISCOVERY | `agent.capability.announce`, `agent.discovery.request`, `agent.discovery.response` | 24h | Capability announcements and queries |
| DIRECT | `agent.direct.>` | 24h | Targeted agent-to-agent delivery |
| WF-{id} | `workflow.{id}.>` | 24h | Per-workflow event ordering (created on demand) |

### Workflow Engine

The Workflow Engine executes multi-step workflows. Each workflow is a sequence of steps, where each step requires a specific capability.

**Lifecycle:**

```
CREATED → RUNNING → COMPLETED
                ↓
              FAILED
```

**How it works:**

1. A `workflow.start` event triggers a new workflow
2. The engine generates a workflow UUID v7 and persists initial state in JetStream KV (`workflow-state` bucket)
3. Creates a per-workflow stream `WF-{id}`
4. Dispatches step 0: finds an agent with the required capability, sends `workflow.step` directly to it
5. Waits for `workflow.step.result` from the agent
6. On success: records result, dispatches next step with all previous results available
7. On final step success: publishes `workflow.complete`
8. On any step failure or timeout: publishes `workflow.failed`

**Key behaviors:**

- **Result chaining** — each step receives the results of all previous steps, enabling agents to build on prior work
- **Step timeouts** — configurable per step (default 30 seconds). If an agent doesn't respond, the workflow fails
- **State persistence** — workflow state is stored in JetStream KV with optimistic locking (Compare-And-Swap)
- **Fail-fast** — no retries, no compensation. If a step fails, the workflow fails immediately

### Capability Discovery

The discovery system enables agents to find each other by capability rather than by identity.

Two components work together:

**Announcer** — listens for `agent.register` and `agent.unregister` events. When an agent registers, the announcer publishes `agent.capability.announce` with its capabilities. When an agent unregisters, it publishes an empty capability list.

**Handler** — listens for `agent.discovery.request` events. When a request comes in, it queries the registry for agents matching the requested capability and publishes `agent.discovery.response` with the matches. Responses are correlated using `correlation_id`.

### Health Monitor

The Health Monitor tracks agent availability using a ping/pong protocol.

**Cycle:**

1. Every 10 seconds (configurable), the monitor sends `agent.health.ping` to all ACTIVE and UNHEALTHY agents
2. Agents respond with `agent.health.pong` containing their status
3. Each missed ping increments a counter
4. After 3 missed pings: agent transitions to UNHEALTHY
5. After 6 missed pings: agent transitions to DEAD, is deregistered, and an `agent.error` is published

**Agent state machine:**

```
REGISTERING → ACTIVE ↔ UNHEALTHY → DEAD → UNREGISTERED
```

If an UNHEALTHY agent responds to a ping, it transitions back to ACTIVE and its missed heartbeat counter resets.

### Agent Registry

The registry stores agent metadata in a JetStream KV bucket (`agent-registry`). It tracks:

- Agent identity (ID, type, version, labels)
- Capabilities (name, version, description, parameters)
- Lifecycle status (ACTIVE, UNHEALTHY, DEAD, UNREGISTERED)
- Health metadata (last seen timestamp, missed heartbeat count)

The registry supports capability-based queries — the workflow engine uses `FindByCapability()` to match steps to agents, and the discovery handler uses it to answer discovery requests.

### SDK

The Go SDK provides a high-level API for building agents. It handles:

- Connection to the runtime via NATS
- Agent registration and capability announcement
- Health ping responses
- Workflow step dispatch and handler routing
- Event deduplication via an in-memory LRU idempotency cache
- Graceful shutdown (drain in-flight handlers, unregister)

See [Building Agents](building-agents.md) for the full SDK guide.

## Data Flow

A typical workflow execution:

1. **Registration** — Agent connects and publishes `agent.register`. The runtime records it in the registry and announces its capabilities.
2. **Workflow trigger** — A `workflow.start` event arrives. The workflow engine creates a new workflow and dispatches step 0.
3. **Step dispatch** — The engine finds an agent with the required capability, sends `workflow.step` to it directly.
4. **Step execution** — The agent processes the step and publishes `workflow.step.result`.
5. **Next step** — The engine receives the result, persists it, and dispatches the next step with all previous results.
6. **Completion** — When all steps succeed, `workflow.complete` is published with the full result set.
7. **Health monitoring** — Throughout this process, the health monitor pings agents and tracks their availability.
