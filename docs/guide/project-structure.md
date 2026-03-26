# Project Structure

```
baran-os/
├── core/                    Runtime implementation
│   ├── cmd/baran/           Runtime binary (embedded NATS + all subsystems)
│   ├── eventbus/            EventBus interface + NATS JetStream implementation
│   ├── router/              Event routing (direct, broadcast, capability-based, workflow-scoped)
│   ├── discovery/           Capability discovery protocol (announcer + handler)
│   ├── workflow/            Workflow engine, decision coordinator, step dispatch, timeouts
│   ├── runtime/             Runtime wiring, operator UI (embedded web assets)
│   ├── health/              Health monitoring (ping/pong, agent state transitions)
│   ├── simulation/          EventStore, ReplayEngine, ScenarioEngine, EventInjector
│   └── registry/            Agent and capability registry (JetStream KV-backed)
├── sidecar/                 Sidecar Gateway (REST/SSE/WebSocket → NATS/protobuf)
│   └── cmd/baran-sidecar/   Sidecar binary entrypoint
├── sdk/                     Go SDK for building agents
├── sdks/                    External language SDKs
│   ├── python/              Python SDK (baran-sdk) — async agent API
│   └── typescript/          TypeScript SDK (@baran/sdk) — typed events API
├── protocol/                Protobuf definitions and generated code
│   ├── proto/               Source .proto files
│   └── gen/                 Generated Go code (do not edit)
├── examples/
│   └── wildfire/            End-to-end wildfire emergency example
│       ├── agents/          Three agents: risk, resource, evacuation
│       ├── trigger/         Workflow trigger program
│       ├── scenarios/       Bundled simulation scenarios (wildfire-simulation.json)
│       └── proto/           Domain-specific protobuf definitions
├── docs/                    GitHub Pages site (landing page + documentation)
│   ├── index.html           Landing page (HTML + Tailwind CDN)
│   └── guide/               Docsify documentation
└── Makefile                 Build, test, lint, dev targets
```

## Key Modules

### `core/`

The runtime implementation. Contains all subsystems that make up the Baran OS runtime. Each subsystem is a separate package with its own tests.

The runtime binary at `core/cmd/baran/` wires everything together: starts an embedded NATS server, initializes all subsystems, and exposes a health endpoint.

### `sidecar/`

The Sidecar Gateway — a REST/SSE/WebSocket API that enables agents in any language to participate in the Baran OS network. Translates between HTTP/JSON and NATS/protobuf using the Go SDK internally. Includes agent lifecycle management, PSK authentication, event publishing, SSE streaming, and WebSocket support.

The sidecar binary at `sidecar/cmd/baran-sidecar/` is the entrypoint.

### `sdk/`

The Go SDK for building agents. A separate Go module (`github.com/baran-network/baran-os/sdk`) that depends on `core/` for the EventBus interface. Provides the `Agent` type with handler registration, lifecycle management, health ping responses, and idempotency.

### `sdks/`

External language SDKs that connect via the Sidecar Gateway (HTTP/SSE):

- **`sdks/python/`** — Python SDK (`baran-sdk`). Async agent API with `@agent.on()` decorators. Requires Python 3.10+, depends on `httpx` only.
- **`sdks/typescript/`** — TypeScript SDK (`@baran/sdk`). Typed events with async/await handlers. Requires TypeScript 5.0+, depends on `eventsource` only.

### `protocol/`

Protobuf definitions for the event protocol. Source files are in `proto/agentosprotocol/v1/` — run `make proto` to regenerate Go code into `gen/`. Never edit files in `gen/` directly.

### `examples/wildfire/`

A complete example demonstrating three agents collaborating on a wildfire emergency response. Includes domain-specific protobuf messages, three agent implementations, and a workflow trigger.

## Build Targets

```bash
make build       # Build the runtime binary
make test        # Run all tests
make test-race   # Run tests with race detector
make dev         # Build and run local development runtime
make proto       # Regenerate protobuf code
make check       # Full check (format, lint, test with race detection)
```
