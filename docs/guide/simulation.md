# Event Replay & Simulation

Event replay enables operators to query historical events from JetStream streams and replay completed workflows on an isolated SIMULATION stream. Replayed events never reach live agents — they are fully isolated from production event flows.

## Overview

The simulation subsystem has two components:

- **EventStore**: Queries historical events from existing JetStream streams by time range, event type, workflow ID, or source agent
- **ReplayEngine**: Re-publishes a workflow's events to the SIMULATION stream with configurable speed and replay metadata

```
┌─────────────────────────────────────────────────────────┐
│                    Baran Runtime                        │
│                                                        │
│  ┌────────────┐     ┌──────────────┐                   │
│  │ EventStore │────►│ ReplayEngine │                   │
│  │  (query)   │     │  (publish)   │                   │
│  └─────┬──────┘     └──────┬───────┘                   │
│        │ reads              │ writes                   │
│  ┌─────┴──────┐     ┌──────┴───────┐                   │
│  │ WF-{id}    │     │ SIMULATION   │                   │
│  │ AGENTS     │     │   stream     │                   │
│  │ DISCOVERY  │     └──────────────┘                   │
│  └────────────┘                                        │
└─────────────────────────────────────────────────────────┘
```

## Quickstart

### Prerequisites

- Running Baran OS instance (`make dev`)
- At least one completed workflow (e.g., the [wildfire example](https://github.com/baran-network/baran-os/tree/main/examples/wildfire))

### Query historical events

```bash
# All events from the last hour
curl "http://localhost:8080/api/events?start=$(date -u -v-1H +%Y-%m-%dT%H:%M:%SZ)"

# Events for a specific workflow
curl "http://localhost:8080/api/events/workflows/{workflow_id}"

# Filter by event type
curl "http://localhost:8080/api/events?start=2026-03-21T00:00:00Z&type=workflow.start"

# Filter by source agent
curl "http://localhost:8080/api/events?start=2026-03-21T00:00:00Z&source_agent=agent-123"
```

### Replay a workflow

```bash
# 1. Create a replay session (max speed, speed=0)
curl -X POST http://localhost:8080/api/replay/sessions \
  -H "Content-Type: application/json" \
  -d '{"workflow_id": "YOUR_WORKFLOW_ID"}'

# 2. Stream replay events via SSE
curl -N http://localhost:8080/api/replay/sessions/{session_id}/stream

# 3. Check session status
curl http://localhost:8080/api/replay/sessions/{session_id}

# 4. Stop a running session
curl -X POST http://localhost:8080/api/replay/sessions/{session_id}/stop
```

### Replay at real-time speed

```bash
curl -X POST http://localhost:8080/api/replay/sessions \
  -H "Content-Type: application/json" \
  -d '{"workflow_id": "YOUR_WORKFLOW_ID", "speed": 1.0}'
```

Speed factors: `0` = max speed (no delays), `1.0` = real-time, `2.0` = 2x, `5.0` = 5x, `10.0` = 10x.

## REST API

### Event Store

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/events` | Query historical events with filters |
| GET | `/api/events/workflows/{id}` | Get all events for a workflow |

**Query parameters for `GET /api/events`**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start` | RFC3339 | yes | Start of time range |
| `end` | RFC3339 | no | End of time range (default: now) |
| `type` | string | no | Event type filter (e.g., `workflow.start`) |
| `workflow_id` | string | no | Filter by workflow ID |
| `source_agent` | string | no | Filter by source agent ID |
| `limit` | int | no | Max events (default: 1000, max: 10000) |
| `offset` | int | no | Pagination offset (default: 0) |

### Replay Sessions

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/replay/sessions` | Create a replay session |
| GET | `/api/replay/sessions` | List sessions (optional `?state=` filter) |
| GET | `/api/replay/sessions/{id}` | Get session details |
| POST | `/api/replay/sessions/{id}/stop` | Stop a running session |
| GET | `/api/replay/sessions/{id}/stream` | SSE stream of replay events |

**Create session request body**:

```json
{
  "workflow_id": "wf-123",
  "speed": 1.0
}
```

### SSE Event Stream

Connect to `/api/replay/sessions/{id}/stream` for real-time replay events:

```
event: replay.event
data: {"event": {...}, "stream": "WF-wf-123", "sequence": 5, "position": 5, "total": 12}

event: replay.complete
data: {"session_id": "...", "total_replayed": 12}

event: replay.stopped
data: {"session_id": "...", "total_replayed": 5}

event: replay.error
data: {"session_id": "...", "error": "..."}
```

## Replay Session Lifecycle

Sessions transition through these states:

```
PENDING ──► RUNNING ──► COMPLETED
                   ├──► STOPPED  (operator request)
                   └──► ERROR    (publish failure)
```

- **PENDING**: Session created, events loaded, waiting to start
- **RUNNING**: Events being published to SIMULATION stream
- **COMPLETED**: All events replayed successfully
- **STOPPED**: Operator stopped the session before completion
- **ERROR**: A publish failure occurred during replay

## Replay Isolation

Every replayed event is published to the **SIMULATION** stream with metadata that distinguishes it from live events:

| Metadata Key | Value | Description |
|-------------|-------|-------------|
| `simulation.replay` | `"true"` | Marks the event as replayed |
| `simulation.session_id` | UUID | Session that produced this event |
| `simulation.original_timestamp` | nanoseconds | Original event timestamp |
| `simulation.original_id` | UUID | Original event ID |

Each replayed event receives a new UUID v7 ID. Original IDs are preserved in metadata. Replayed events never appear on live streams (AGENTS, WF-{id}, DISCOVERY, etc.).

## Coordination Events

The ReplayEngine publishes coordination events on the SIMULATION stream:

| Event Type | Payload | When |
|-----------|---------|------|
| `simulation.replay.start` | `SimulationReplayStartPayload` | Session begins |
| `simulation.replay.stop` | `SimulationReplayStopPayload` | Session stopped or errored |
| `simulation.replay.complete` | `SimulationReplayCompletePayload` | All events replayed |

These events are defined in `protocol/proto/agentosprotocol/v1/simulation.proto`.

## SIMULATION Stream

The SIMULATION stream is registered in the `StreamRegistry` alongside other system streams:

| Property | Value |
|----------|-------|
| Name | `SIMULATION` |
| Subjects | `simulation.>` |
| Max Age | 24h |
| Retention | Limits (same as other system streams) |

## Architecture Notes

- **EventStore reads directly from JetStream** — it does not subscribe through the EventBus. This avoids coupling the query path to the routing path and provides efficient access to historical data using ordered consumers with `DeliverByStartTimePolicy`.
- **Sessions are ephemeral** — stored in memory, not persisted across restarts. A future spec may add session persistence.
- **No router involvement** — replayed events are published directly to the SIMULATION stream, bypassing the router entirely. This ensures zero impact on live routing latency.
- **New UUIDs for replayed events** — NATS deduplication window (2 min) would silently drop events with original IDs, so each replayed event gets a new UUID v7.
