# Operator UI

The Baran OS Operator UI is a real-time web dashboard for monitoring and managing your agent network. It connects to the runtime via HTTP and SSE, providing live visibility without requiring direct NATS access.

## Features

| View | What you get |
|------|-------------|
| **Network Dashboard** | All agents, health statuses, capability filters, resource usage, and live event feed per agent |
| **Event Flow Monitor** | Live SSE event stream with filtering, pause/resume buffering, and workflow timeline visualization |
| **Federation View** | Interactive graph of federated clusters, relay connections, and cross-cluster capabilities |
| **Visual Simulator** | Event replay at adjustable speed, scenario execution, and manual event injection |
| **Human Decisions** | Pending decision cards with approve/reject, conflict grouping, and decision history |

## Quick Start

### Prerequisites

- Node.js 24+
- A running Baran OS runtime (`./baran`)

### Setup

```bash
cd ui
cp .env.example .env.local
# Edit .env.local — set BARAN_RUNTIME_URL and BARAN_UI_TOKEN
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000). The UI redirects to `/dashboard` by default.

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BARAN_RUNTIME_URL` | Base URL of the Baran runtime HTTP API | `http://localhost:8080` |
| `BARAN_UI_TOKEN` | Pre-shared key for operator API authentication | — |

## Architecture

The UI is a **standalone Next.js 16+ application** (`ui/`) that consumes the runtime's operator API:

```
Browser → Next.js UI → Baran Runtime HTTP/SSE → NATS JetStream
```

The runtime exposes a set of read-only operator endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /api/agents` | All registered agents from `agent-registry` KV |
| `GET /api/agents/{id}` | Single agent detail |
| `GET /api/workflows` | All workflow states from `workflow-state` KV |
| `GET /api/workflows/{id}` | Single workflow detail |
| `GET /api/capabilities` | Capability catalog |
| `GET /api/stats` | Aggregate stats (agent counts, throughput, decisions) |
| `GET /api/events/stream` | SSE stream of all events with `Last-Event-ID` recovery |

Decision responses write through the existing human-in-the-loop API:

| Endpoint | Description |
|----------|-------------|
| `GET /api/decisions` | All pending decisions |
| `POST /api/decisions/{id}/respond` | Approve or reject a decision |

## Network Dashboard

The dashboard (`/dashboard`) shows:

- **Summary Panel** — total/healthy/degraded/offline agent counts, event throughput, active workflows, pending decisions
- **Agent Table** — sortable by name, type, status, capabilities, last heartbeat. Status badges (green/yellow/red). A2A origin badge for external agents.
- **Agent Filters** — filter by name substring, status, capability, type
- **Agent Detail** — slide-out panel with Info tab, Capabilities (grouped by taxonomy category), Resources (CPU/memory/pending steps), and Live Events filtered by agent

Updates are driven by SSE events — no polling required for agent state changes.

## Event Flow Monitor

The Events page (`/events`) provides:

- **Live Stream tab** — virtualized event list supporting 100 events/sec with `requestAnimationFrame` batching and a 10,000 event ring buffer
- **Pause/Resume** — incoming events buffer in memory while paused; resume flushes them in order
- **Filters** — by event type prefix (e.g., `agent.`), source/target agent, workflow ID
- **High-traffic indicator** — badge appears when throughput exceeds 50 events/sec
- **Workflow Timeline tab** — select a workflow and see its steps as a horizontal timeline with duration, status colors, and step detail on click

## Federation View

The Federation page (`/federation`) renders an interactive graph (React Flow):

- **Cluster nodes** — each federated node shown as a card with agent count, status badge, and expand/collapse
- **Relay edges** — connections colored by status (active=green, inactive=gray) with latency label
- **Cluster detail** — click a node to see its agents (with A2A origin badge), capabilities, and aliases
- **Empty state** — friendly message when federation is not configured

## Visual Simulator

The Simulator page (`/simulator`) provides three tabs:

### Event Replay

Select a completed workflow and replay its events at 1x, 2x, 5x, 10x, or Max speed. A progress bar tracks replay position. All replayed events appear with a `[SIM]` badge in the simulation stream.

### Scenarios

Browse and run registered scenarios (from the Scenario Runner). Cards show name, description, and step count. Execution progress streams live via SSE.

### Manual Injection

Inject custom events into the SIMULATION stream: select event type, source/target agents, and provide a JSON payload. The injected event appears immediately in the simulation stream with a `[SIM]` badge.

All simulator events flow on the isolated `SIMULATION` JetStream stream and are visually distinct from live traffic.

## Human Decisions

The Decisions page (`/decisions`) shows:

- **Pending tab** — decision cards with the prompt, workflow context, resource IDs, conflict indicators, comment field, and approve/reject buttons. Related decisions sharing a conflict ID are grouped together.
- **History tab** — resolved decisions with outcome and timestamp

New pending decisions arrive in real time via SSE.

## Performance

| Metric | Target | Implementation |
|--------|--------|----------------|
| Event throughput | 100 events/sec | RAF batching (100 events/frame) + virtual scrolling |
| Initial load | <500ms | Next.js App Router + SSR |
| Filter response | <200ms | Client-side filtering on in-memory buffer |
| Ring buffer cap | 10,000 events | Oldest events dropped when cap reached |

## Development

```bash
cd ui
npm run dev      # Development server with hot reload
npm run build    # Production build
npm run lint     # ESLint check
npm run format   # Prettier format
```
