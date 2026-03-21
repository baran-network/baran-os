# Wildfire Emergency Management Example

A complete end-to-end example demonstrating Baran OS agent lifecycle, capability discovery, workflow orchestration, step dispatch with result chaining, and graceful shutdown.

## Scenario

A wildfire is reported in the Sierra Nevada. Three agents collaborate sequentially:

1. **Risk Estimation** — Analyzes the incident and produces a risk assessment (threat level, spread rate, affected zones)
2. **Resource Allocation** — Reads the risk assessment and assigns firefighting resources scaled to the threat
3. **Evacuation Planning** — Reads both prior results and produces an evacuation plan with zones, routes, and shelters

Each agent receives the original incident data as step input, plus all previous step results for chaining.

## Prerequisites

- Go 1.22+
- Baran OS repository cloned
- Port 4222 free (no other NATS server running)

## Quick Start

### 1. Build the runtime

```bash
cd /path/to/baran-os
go build -o baran ./core/cmd/baran
```

### 2. Start the runtime

```bash
./baran -log-level debug
```

The runtime starts an embedded NATS server on `:4222` and a health endpoint on `:8080`.

### 3. Start the agents (in separate terminals)

```bash
# Terminal 2: Risk estimation agent
cd examples/wildfire
go run ./agents/risk

# Terminal 3: Resource allocation agent
cd examples/wildfire
go run ./agents/resource

# Terminal 4: Evacuation planning agent
cd examples/wildfire
go run ./agents/evacuation
```

Each agent logs: registration, capability announcement, and readiness.

### 4. Trigger the workflow

```bash
cd examples/wildfire
go run ./trigger
```

### 5. Expected output

**Trigger output:**
```
Workflow started. Waiting for completion...

Workflow completed successfully!
Workflow ID: <uuid>
Steps completed: 3

--- Step 0 (agent: <uuid>) ---
  Status: STEP_STATUS_SUCCESS
  Risk Score: 0.80
  Threat Level: THREAT_LEVEL_SEVERE
  Spread Rate: 30.0 ha/hr
  Affected Zones: [Zone-A Zone-B Zone-C]

--- Step 1 (agent: <uuid>) ---
  Status: STEP_STATUS_SUCCESS
  Fire Trucks: 12
  Crews: 8
  Aircraft: 3
  Staging Area: Base Camp Alpha - Highway 395
  Response Time: 25 min

--- Step 2 (agent: <uuid>) ---
  Status: STEP_STATUS_SUCCESS
  Evacuees: 1830
  Shelters: [Community Center - Reno High School Gym - Carson City Fairgrounds - Minden]
  Zone "Zone-A (Residential)" (priority 1): pop 1200, route: Highway 395 South
  Zone "Zone-B (Commercial)" (priority 2): pop 450, route: Interstate 80 West
  Zone "Zone-C (Rural)" (priority 3): pop 180, route: Forest Service Road 3
```

**Each agent terminal shows:**
- Step received with input/previous results
- Processing (2-second simulated delay)
- Result published

### 6. Graceful shutdown

Press `Ctrl+C` on any agent — it finishes in-flight work, unregisters, and exits cleanly.
Press `Ctrl+C` on the runtime — it shuts down all subsystems.

## What You Just Saw

- **Agent lifecycle**: Each agent registered on start, announced its capability, and unregistered on shutdown
- **Capability discovery**: The workflow engine matched each step to the right agent by capability name
- **Workflow orchestration**: Three sequential steps executed in order with result chaining through `PreviousResults`
- **Event-driven coordination**: All communication flowed through the event bus — no direct RPC calls
- **Graceful shutdown**: Clean unregistration and in-flight handler completion on SIGINT

## Running the Integration Test

```bash
cd examples/wildfire
go test ./... -timeout 60s -v
```

This runs the full workflow end-to-end in-process with an embedded NATS server — no manual setup required.

## Multi-Node Federation Example

With Baran OS federation, you can split agents across two runtime nodes and run the workflow transparently across them. This demonstrates cross-node capability sharing and event relay.

### Setup: Node A (fire detection) + Node B (evacuation planning)

**Terminal 1 — Start Node A (seed node, fire detection side):**

```bash
./baran \
  --nats-port 4222 \
  --health-port 8080 \
  --federation-psk "wildfire-demo"
```

**Terminal 2 — Start Node B (joins federation, evacuation side):**

```bash
./baran \
  --nats-port 4223 \
  --health-port 8081 \
  --federation-seeds "127.0.0.1:7422" \
  --federation-psk "wildfire-demo"
```

Wait ~2 seconds and verify both nodes see each other:

```bash
curl http://localhost:8080/api/federation/nodes
# Both nodes appear with status "ACTIVE"
```

**Terminal 3 — Risk agent (connects to Node A):**

```bash
NATS_URL=nats://localhost:4222 go run ./agents/risk
```

**Terminal 4 — Resource agent (connects to Node A):**

```bash
NATS_URL=nats://localhost:4222 go run ./agents/resource
```

**Terminal 5 — Evacuation agent (connects to Node B):**

```bash
NATS_URL=nats://localhost:4223 go run ./agents/evacuation
```

**Terminal 6 — Trigger (connects to Node A):**

```bash
NATS_URL=nats://localhost:4222 go run ./trigger
```

The workflow starts on Node A. Steps 0 and 1 run on local agents (risk + resource). Step 2 requires `evacuation.plan`, which only exists on Node B — the router relays the step transparently and the result flows back to Node A to complete the workflow.

> **Note**: The agents in this example read `NATS_URL` from the environment. If your agents don't yet support this, connect them directly to the NATS server address for their target node.

### What federation adds

- **Capability discovery**: Node A's workflow engine sees the evacuation capability registered on Node B
- **Transparent relay**: When step 2 dispatches, the router detects the agent is remote and relays the event via NATS leaf node connection to Node B
- **Result routing**: The step result published on Node B is relayed back to Node A's workflow stream, completing the step
- **Health monitoring**: If Node B goes down, Node A marks it UNHEALTHY (3 missed heartbeats) then DEAD (6 missed heartbeats), and purges its capabilities

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `port 4222 already in use` | Stop any running NATS server or Baran runtime |
| `no agent found for capability` | Ensure all three agents are running before triggering |
| `workflow timeout` | Check agent logs for errors; increase step timeout if needed |
| `connection refused` | Verify the runtime is running and NATS is accessible |
| `module not found` | Run from within the `examples/wildfire/` directory |
| Federation: `no remote capabilities` | Wait 2-3s for capability sync, then check `/api/federation/nodes` |
