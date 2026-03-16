# Project Structure

```
baran-os/
├── core/                    Runtime implementation
│   ├── cmd/baran/           Runtime binary (embedded NATS + all subsystems)
│   ├── eventbus/            EventBus interface + NATS JetStream implementation
│   ├── router/              Event routing (direct, broadcast, capability-based, workflow-scoped)
│   ├── discovery/           Capability discovery protocol (announcer + handler)
│   ├── workflow/            Workflow engine (state machine, step dispatch, timeouts)
│   ├── health/              Health monitoring (ping/pong, agent state transitions)
│   └── registry/            Agent and capability registry (JetStream KV-backed)
├── sdk/                     Go SDK for building agents
├── protocol/                Protobuf definitions and generated code
│   ├── proto/               Source .proto files
│   └── gen/                 Generated Go code (do not edit)
├── examples/
│   └── wildfire/            End-to-end wildfire emergency example
│       ├── agents/          Three agents: risk, resource, evacuation
│       ├── trigger/         Workflow trigger program
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

### `sdk/`

The Go SDK for building agents. A separate Go module (`github.com/baran-network/baran-os/sdk`) that depends on `core/` for the EventBus interface. Provides the `Agent` type with handler registration, lifecycle management, health ping responses, and idempotency.

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
