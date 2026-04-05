# Baran OS

**A distributed runtime for autonomous agent coordination.**

Named after [Paul Baran](https://en.wikipedia.org/wiki/Paul_Baran), pioneer of distributed networks, Baran OS is an event-driven runtime where autonomous agents — AI-powered or not — discover each other, collaborate through typed events, and execute multi-step workflows without ever communicating directly.

[![Version](https://img.shields.io/badge/version-v0.5.0-blue)](CHANGELOG.md)
[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![NATS](https://img.shields.io/badge/NATS-JetStream-27AAE1?logo=nats.io&logoColor=white)](https://nats.io)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-baran--network.github.io-purple)](https://baran-network.github.io/baran-os/)

## Why Baran

Coordinating multiple autonomous agents is hard. When agents need to collaborate — share results, hand off work, make decisions together — you need infrastructure that handles discovery, routing, sequencing, failure, and state. Building this from scratch for every project is wasteful and error-prone.

Baran OS provides the coordination layer so you can focus on what each agent does, not how they find and talk to each other.

**Core principles:**

- **Agents are external processes** — bring your own language, framework, and logic. Baran only coordinates.
- **All communication flows through the event bus** — no direct agent-to-agent calls. This makes the system observable, auditable, and resilient.
- **Immutable events, stateless agents** — the runtime owns all state. Agents are disposable and horizontally scalable.
- **Typed protocol** — protobuf-defined events with strict payload typing. No stringly-typed chaos.

## Use Cases

Baran OS is designed for scenarios where multiple autonomous agents — with different capabilities, frameworks, and even intelligence models — need to coordinate in real time.

### Emergency Management

Multiple agents collaborate to respond to a crisis: a sensor detects a wildfire, an AI agent estimates risk, a rule-based agent allocates resources, and a human approves the evacuation plan. Each agent contributes its specialty without knowing about the others.

With federation, networks at different levels coordinate hierarchically. A community node detects a wildfire and handles the initial response locally. When the situation exceeds its capacity, it relays a resource request to the provincial node, which has visibility over a wider pool of agents and resources. Each network operates autonomously but can escalate through cross-node event relay.

```
┌─────────────────┐         ┌─────────────────┐
│  Community Node │         │ Provincial Node │
│                 │  relay  │                 │
│  sensor ──→ AI  ├────────►│  resource-pool  │
│  risk ──→ evac  │◄────────┤  coordination   │
│                 │         │  mutual-aid     │
└─────────────────┘         └─────────────────┘
```

### Autonomous Coding

A coding workflow where AI agents divide the work: a planner breaks down the task, a coder writes the implementation, a reviewer checks for issues, and a tester validates the result. Each agent can use a different LLM or strategy, coordinated as workflow steps with result chaining.

### Agent Swarms

Large-scale agent coordination where dozens of agents with different capabilities register dynamically, discover each other through the capability registry, and self-organize into workflows. Baran's broadcast routing, capability-based discovery, and workflow engine provide the infrastructure swarms need.

### AI Meets Traditional Systems

Baran doesn't require agents to be AI-powered. A workflow can mix LLM-based agents (LangGraph, CrewAI, custom) with rule engines, sensor feeds, legacy services, and human decision points — all speaking the same event protocol. An IoT sensor triggers a workflow, an AI agent analyzes the data, a heuristic agent applies business rules, and a human makes the final call.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        Baran Runtime                          │
│                                                               │
│  ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌────────────┐  │
│  │  Router  │  │ Workflow  │  │ Decision │  │ Capability │  │
│  │          │  │  Engine   │  │Coordinator│  │ Discovery  │  │
│  └────┬─────┘  └─────┬─────┘  └─────┬────┘  └─────┬──────┘  │
│       │               │              │             │          │
│  ┌────┴───────────────┴──────────────┴─────────────┴───────┐  │
│  │              Event Bus (NATS JetStream)                   │  │
│  └────┬───────────────┬──────────────┬─────────────┬───────┘  │
│       │               │              │             │          │
└───────┼───────────────┼──────────────┼─────────────┼──────────┘
        │               │              │             │
  ┌─────┴─────┐  ┌──────┴──────┐  ┌───┴──────┐  ┌──┴────────┐
  │  Agent A  │  │   Agent B   │  │ Operator │  │  Agent C   │
  │  (AI/LLM) │  │ (heuristic) │  │  (human) │  │  (sensor)  │
  └───────────┘  └─────────────┘  └──────────┘  └────────────┘
```

Agents connect to the runtime, register their capabilities, and receive workflow steps matched to those capabilities. Human operators interact through a built-in web UI to approve or reject workflow steps. The runtime handles routing, sequencing, state, health monitoring, decision coordination, and failure detection. Agents handle domain logic.

## Getting Started

### Prerequisites

- Go 1.22+
- Port 4222 free (embedded NATS)

### Build and run the runtime

```bash
git clone https://github.com/baran-network/baran-os.git
cd baran-os
make build
./baran
```

The runtime starts an embedded NATS server on `:4222` and a health endpoint on `:8080`.

### Run the wildfire example

The [wildfire example](examples/wildfire/) demonstrates three agents collaborating on an emergency response: risk estimation → resource allocation → evacuation planning.

```bash
# Terminal 1: Start the runtime
./baran -log-level debug

# Terminal 2-4: Start each agent
go run ./examples/wildfire/agents/risk
go run ./examples/wildfire/agents/resource
go run ./examples/wildfire/agents/evacuation

# Terminal 5: Trigger the workflow
go run ./examples/wildfire/trigger
```

See the [wildfire README](examples/wildfire/README.md) for full details and expected output.

### Build an agent

The Go SDK lets you build an agent in under 20 lines:

```go
package main

import (
    "context"
    "github.com/baran-network/baran-os/sdk"
)

func main() {
    agent := sdk.New("my-agent", "analyzer", "1.0.0")

    agent.Handle(sdk.Capability{
        Name:    "analyze-data",
        Version: "1.0.0",
    }, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
        // Your logic here — call an LLM, run heuristics, read sensors
        return []byte(`{"result": "analysis complete"}`), nil
    })

    agent.Run(context.Background())
}
```

The SDK handles connection, registration, capability announcement, health pings, step dispatch, idempotency, and graceful shutdown.

### Capability Taxonomy

Baran OS ships with 48 well-known capabilities across 8 categories (`nlp`, `code`, `vision`, `data`, `decision`, `communication`, `orchestration`, `security`). Capabilities follow dot-notation: `nlp.summarization`, `code.generation`.

When an agent registers a standard capability, the registry auto-fills its category, input types, and output types from the catalog. Discovery supports glob patterns — `nlp.*` returns all NLP agents. Agents can also register vendor-namespaced capabilities (`acme.wildfire.risk_assessment`) when no standard entry fits.

Federated nodes can define **capability aliases** to map equivalent vendor capabilities across organizations, enabling cross-node discovery without requiring identical naming.

See the [Capability Taxonomy guide](https://baran-network.github.io/baran-os/#/guide/taxonomy) for the full catalog and vendor namespace rules.

### A2A Gateway — Interoperability with External Agents

The **A2A Gateway** bridges [Google's A2A protocol](https://google.github.io/A2A/) to the Baran OS network:

```bash
# Start the A2A gateway
baran-a2a --nats-url nats://localhost:4222 --a2a-port 8090 --psk my-secret

# External A2A clients discover Baran agents
curl http://localhost:8090/.well-known/agent-card.json

# External A2A clients invoke Baran agents via JSON-RPC 2.0
curl -X POST http://localhost:8090/ \
  -H "Authorization: Bearer my-secret" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"message/send","id":"1",
       "params":{"message":{"role":"user","parts":[{"text":"..."}]},
                 "configuration":{"skill":"nlp.summarization"}}}'
```

External A2A agents can also be onboarded into Baran — the gateway fetches their Agent Cards, maps skills to the Baran taxonomy, and registers them as virtual participants so Baran workflows dispatch to them transparently.

See the [A2A Gateway guide](https://baran-network.github.io/baran-os/#/guide/a2a-gateway) for details.

### Sidecar Gateway — Any Language

Agents written in any language can join the network via the **Sidecar Gateway**, which translates between HTTP/JSON and the native NATS/protobuf protocol:

```bash
# Start the sidecar
baran-sidecar --nats-url nats://localhost:4222 --psk my-secret-key

# Register an agent via HTTP
curl -X POST http://localhost:9090/agents \
  -H "Authorization: Bearer my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-agent", "agent_type": "analyzer", "version": "1.0.0",
       "capabilities": [{"name": "echo.text", "version": "1.0.0"}]}'
```

External agents use REST for publishing, SSE or WebSocket for receiving events. See the [Sidecar Gateway guide](https://baran-network.github.io/baran-os/#/guide/sidecar-gateway) for details.

### SDKs

| Language   | Package | Status |
|------------|---------|--------|
| Go         | `sdk/`  | Available (native) |
| Python     | `baran-sdk` (pip) | Available |
| TypeScript | `@baran/sdk` (npm) | Available |

The Go SDK connects directly to NATS. Python and TypeScript SDKs connect via the Sidecar Gateway (HTTP/SSE) — no NATS or protobuf dependency required.

## Project Structure

```
baran-os/
├── core/               Runtime implementation
│   ├── cmd/baran/      Runtime binary (embedded NATS + all subsystems)
│   ├── eventbus/       EventBus interface + NATS implementation
│   ├── router/         Event routing (direct, broadcast, capability-based, relay)
│   ├── discovery/      Capability discovery protocol
│   ├── workflow/       Workflow engine, decision coordinator, step dispatch
│   ├── federation/     Multi-node federation (discovery, relay, capability sync)
│   ├── simulation/     EventStore, ReplayEngine, ScenarioEngine, EventInjector
│   ├── runtime/        Runtime wiring, operator UI (embedded web assets)
│   ├── health/         Health monitoring
│   └── registry/       Agent and capability registry (KV-backed)
├── sidecar/            Sidecar Gateway (REST/SSE/WebSocket → NATS/protobuf)
│   └── cmd/baran-sidecar/  Sidecar binary entrypoint
├── a2a/                A2A Gateway core package
│   └── cmd/baran-a2a/  A2A Gateway binary entrypoint
├── core/taxonomy/      Capability taxonomy (48 standard entries, vendor validation)
├── sdk/                Go SDK for building agents
├── sdks/               External language SDKs
│   ├── python/         Python SDK (baran-sdk)
│   └── typescript/     TypeScript SDK (@baran/sdk)
├── protocol/           Protobuf definitions and generated code
├── examples/wildfire/  End-to-end wildfire emergency example (single + multi-node)
└── Makefile            Build, test, lint, dev targets
```

## Status

Baran OS **v0.6.0** adds the Capability Taxonomy (48 well-known capabilities, vendor namespaces, federation aliases) and the A2A Gateway for interoperability with external A2A agents.
See the full [changelog](CHANGELOG.md) and the [documentation site](https://baran-network.github.io/baran-os/).

**What works today:**
- Agent lifecycle (registration, health monitoring, unregistration)
- Event routing (direct, broadcast, workflow-scoped, capability-based)
- Capability discovery and registry
- Workflow engine (sequential steps, result chaining, timeouts, failure detection)
- Human-in-the-loop decisions (approval gates, conflict detection, operator web UI)
- **Multi-node federation** — node discovery, capability sharing, cross-node event relay, health monitoring, automatic dead-node cleanup
- **Event replay & simulation** — query historical events, replay completed workflows on an isolated SIMULATION stream, real-time SSE streaming
- **Scenario runner** — define and execute simulation scenarios with scripted event sequences, per-step delays, conditions, full REST API, and SSE streaming
- **Sidecar Gateway** — REST/SSE/WebSocket API for external agents in any language, JSON↔protobuf translation, PSK authentication
- **Python SDK** (`baran-sdk`) — async agent with decorator-based event handlers, SSE streaming, automatic lifecycle
- **TypeScript SDK** (`@baran/sdk`) — typed events, async/await handlers, SSE streaming
- Single-binary runtime with embedded NATS
- Go SDK for building agents
- End-to-end wildfire example (single-node, multi-node federation, and simulation scenarios)
- Documentation site with quickstart, SDK reference, event catalog, federation guide, sidecar gateway guide, and simulation guide

**What's coming:**
- Capability taxonomy and A2A (Agent-to-Agent) gateway for cross-platform interoperability
- LLM agent example — autonomous coding workflow with LangGraph integration
- Operator UI — network dashboard, federation view, and visual simulator
- Distribution — goreleaser binaries, container images, and SDK packages

## Development

```bash
make build       # Build the runtime binary
make test        # Run all tests
make test-race   # Run tests with race detector
make dev         # Build and run local development runtime
make proto       # Regenerate protobuf code
make check       # Full check (format, lint, test with race detection)
```

## License

[MIT](LICENSE) — Carlos Molina Beltrán
