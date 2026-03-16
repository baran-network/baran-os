# Getting Started

This guide takes you from zero to a running Baran OS runtime with the wildfire emergency example.

## Prerequisites

- **Go 1.22+** — [install Go](https://go.dev/doc/install)
- **Port 4222 free** — the embedded NATS server binds to this port
- **Port 8080 free** — the health endpoint binds to this port

## Install the Runtime

```bash
go install github.com/baran-network/baran-os/cmd/baran@latest
```

This installs the `baran` binary to your `$GOPATH/bin`.

## Start the Runtime

```bash
baran
```

You should see output like:

```
INF starting Baran OS runtime
INF embedded NATS server started port=4222
INF JetStream enabled store_dir=./baran-data
INF health endpoint listening addr=127.0.0.1:8080
INF runtime ready
```

The runtime starts an embedded NATS server with JetStream, creates the required streams and KV buckets, and begins health monitoring.

### Runtime Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-nats-port` | 4222 | NATS server port |
| `-nats-store-dir` | `./baran-data` | JetStream persistence directory |
| `-health-port` | 8080 | HTTP health endpoint port |
| `-log-level` | `info` | Log level: debug, info, warn, error |
| `-workflow-timeout` | 30s | Default step timeout |
| `-shutdown-grace` | 15s | Graceful shutdown timeout |

All flags have corresponding environment variables (e.g., `BARAN_NATS_PORT`, `BARAN_LOG_LEVEL`).

## Run the Wildfire Example

The [wildfire example](https://github.com/baran-network/baran-os/tree/main/examples/wildfire) demonstrates three agents collaborating on an emergency response workflow:

1. **Risk Estimation** — assesses wildfire severity, calculates threat level and spread rate
2. **Resource Allocation** — assigns emergency resources based on the risk assessment
3. **Evacuation Planning** — creates evacuation zones and shelter assignments based on risk and resources

### Start the Agents

Open separate terminals for each agent:

```bash
# Terminal 1: Risk estimation agent
go run ./examples/wildfire/agents/risk

# Terminal 2: Resource allocation agent
go run ./examples/wildfire/agents/resource

# Terminal 3: Evacuation planning agent
go run ./examples/wildfire/agents/evacuation
```

Each agent connects to the runtime, registers its capability, and waits for workflow steps.

### Trigger the Workflow

In a new terminal:

```bash
go run ./examples/wildfire/trigger
```

The trigger creates a three-step workflow with a fictional Sierra Nevada wildfire incident (severity: HIGH, 150 hectares affected, 35 km/h NE wind).

### Expected Output

The trigger program prints the workflow progress:

```
Workflow started: <workflow-id>
Step 0 (risk-estimation): risk=0.80, threat=SEVERE, spread=30.0 ha/hr
Step 1 (resource-allocation): trucks=12, crews=8, aircraft=3
Step 2 (evacuation-planning): evacuees=1830, zones=3
Workflow completed successfully
```

Each step builds on the previous results:
- The risk agent receives the incident and produces a risk assessment
- The resource agent receives the incident + risk assessment and produces a resource plan
- The evacuation agent receives the incident + risk assessment + resource plan and produces an evacuation plan

## What's Running

After starting the runtime and agents, you have:

- **1 runtime** — embedded NATS server, event router, workflow engine, health monitor, capability discovery
- **3 agents** — each registered with one capability, responding to health pings
- **5 JetStream streams** — AGENTS, HEALTH, DISCOVERY, DIRECT, plus one WF-{id} stream for the workflow
- **2 KV buckets** — `agent-registry` (agent state) and `workflow-state` (workflow state)

## Next Steps

- [Architecture](architecture.md) — understand how the runtime components work together
- [Building Agents](building-agents.md) — build your own agent with the Go SDK
- [Event Catalog](event-catalog.md) — reference for all event types and payloads
- [Project Structure](project-structure.md) — navigate the repository
