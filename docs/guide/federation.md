# Federation

Federation enables multiple Baran OS nodes to form a distributed cluster where agents on different nodes discover each other, share capabilities, and execute workflow steps transparently across node boundaries.

## Overview

A federated cluster is a set of Baran OS runtimes connected via NATS leaf node links. Each node:

- Maintains a `node-registry` KV bucket with the health status of all known peers
- Propagates its local agent capabilities to all peers
- Routes events to remote agents automatically when no local match exists

Federation is **opt-in**: a node without `--federation-seeds` runs in standalone mode with no change to existing behavior.

```
┌──────────────────────────┐         ┌──────────────────────────┐
│       Node A             │         │       Node B             │
│                          │  leaf   │                          │
│  Router ──► FedGateway  ├────────►│  FedGateway ──► Router  │
│  AgentReg ◄─ CapSync    │◄────────┤  CapSync ──► AgentReg   │
│  NodeReg  ◄─ NodeMonitor│         │  NodeMonitor ──► NodeReg │
└──────────────────────────┘         └──────────────────────────┘
```

## Quickstart

### 1. Start the seed node

```bash
./baran \
  --federation-psk "my-secret-key"
```

The seed node runs in standalone mode until peers connect. Its leaf node listener starts on port `7422` by default.

### 2. Start a peer node

```bash
./baran \
  --nats-port 4223 \
  --health-port 8081 \
  --federation-seeds "seed-host:7422" \
  --federation-psk "my-secret-key"
```

### 3. Verify the cluster

```bash
curl http://localhost:8080/api/federation/nodes
```

```json
{
  "nodes": [
    {
      "node_id": "019d...",
      "address": "127.0.0.1:7422",
      "status": "ACTIVE",
      "capabilities_count": 2,
      "last_seen": "2026-03-21T18:00:00Z",
      "joined_at": "2026-03-21T17:55:00Z",
      "missed_heartbeats": 0
    },
    {
      "node_id": "019e...",
      "address": "127.0.0.1:7423",
      "status": "ACTIVE",
      "capabilities_count": 1,
      "last_seen": "2026-03-21T18:00:00Z",
      "joined_at": "2026-03-21T17:58:00Z",
      "missed_heartbeats": 0
    }
  ]
}
```

## Configuration Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--federation-seeds` | (empty) | Comma-separated `host:port` seed addresses. Empty = standalone mode. |
| `--federation-psk` | (empty) | Pre-shared key for inter-node authentication. Required when seeds are set. |
| `--federation-heartbeat` | `10s` | How often each node pings its peers. |
| `--federation-unhealthy` | `3` | Missed heartbeats before a node is marked `UNHEALTHY`. |
| `--federation-dead` | `6` | Missed heartbeats before a node is marked `DEAD`. |
| `--federation-relay-timeout` | `30s` | Maximum wait for a relay response before failing the workflow step. |
| `--federation-leaf-port` | `7422` | NATS leaf node listener port (peers connect here). |

## Node State Machine

Each remote node in the registry transitions through these states:

```
           join              missed >= unhealthy_threshold
UNKNOWN ──────► ACTIVE ──────────────────────────────► UNHEALTHY
                   ▲                                       │
                   │ heartbeat received                    │ missed >= dead_threshold
                   └───────────────────────────────────── ▼
                                                        DEAD ──► (purged after CleanupTTL)
```

- **ACTIVE**: node is reachable and responding to health pings
- **UNHEALTHY**: node has missed several heartbeats but has not exceeded the DEAD threshold
- **DEAD**: node has exceeded the DEAD threshold; its remote capabilities are purged from all peers
- **Cleanup**: DEAD nodes are removed from the `node-registry` after `CleanupTTL` (default: 5 minutes)

## How It Works

### Node Discovery

When a node starts with seeds configured, it:

1. Announces itself by publishing a `federation.node.register` event with its node ID, address, version, and capability count
2. All peers that receive this event upsert it into their local `node-registry` and propagate it to other known peers (one hop only — loop detection via `propagated` metadata)
3. When a node shuts down gracefully, it publishes `federation.node.unregister`; all peers mark it as `DEAD` immediately

### Capability Sharing

Each node subscribes to its own `agent.capability.announce` events. When an agent registers a capability:

1. The `CapabilitySync` component publishes a `federation.capability.announce` event to all peers via the leaf node transport
2. Peers store the remote agent in their local `agent-registry` under the key `remote.{nodeID}.{agentID}` with `Origin: "remote"`
3. Capability queries (`FindByCapability`) return both local and remote agents transparently

### Cross-Node Event Relay

The `DefaultRouter` has been extended with relay awareness:

- **Direct routing** (`route.direct`): if the target agent is remote, the event is serialized as a `FederationRelayPayload` and published to `federation.relay.{targetNodeID}.{event.type}` via the leaf node transport
- **Capability routing** (`route.capability`): local agents are always preferred; remote agents are only used when no local match exists

The receiving node's gateway subscribes to `federation.relay.{localNodeID}.>`, deserializes the payload, and routes the original event through its local router.

### Health Monitoring

Each node runs a `NodeMonitor` goroutine that:

- Publishes `federation.node.health.ping` to all known ACTIVE/UNHEALTHY peers at each heartbeat interval
- Listens for `federation.node.health.pong` responses and records successful heartbeats
- Increments `missed_heartbeats` for nodes that don't respond; transitions ACTIVE→UNHEALTHY→DEAD per configured thresholds

## REST API

### `GET /api/federation/nodes`

Returns all known nodes in the `node-registry`.

**Response** (200):
```json
{
  "nodes": [
    {
      "node_id": "019d...",
      "address": "127.0.0.1:7422",
      "status": "ACTIVE",
      "capabilities_count": 3,
      "last_seen": "2026-03-21T18:00:00Z",
      "joined_at": "2026-03-21T17:55:00Z",
      "missed_heartbeats": 0,
      "version": "0.2.0"
    }
  ]
}
```

Returns an empty array when federation is disabled (standalone mode).

### `GET /healthz`

The health endpoint includes a `federation` key when the gateway is initialized:

```json
{
  "status": "ok",
  "federation": {
    "enabled": true,
    "node_count": 3,
    "healthy_nodes": 3
  }
}
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Baran Runtime (Node A)                     │
│                                                               │
│  ┌──────────┐  ┌───────────┐  ┌─────────────────────────┐   │
│  │  Router  │  │ Workflow  │  │   Federation Gateway     │   │
│  │  +relay  │  │  Engine   │  │  NodeMonitor  CapSync    │   │
│  └────┬─────┘  └─────┬─────┘  │  EventRelay   NodeReg   │   │
│       │               │        └──────────┬──────────────┘   │
│  ┌────┴───────────────┴──────────────────┐│                  │
│  │        Event Bus (NATS JetStream)      ││                  │
│  └───────────────────────────────────────┘│                  │
└──────────────────────────────────────────-┤------------------┘
                                            │ NATS Leaf Node
┌───────────────────────────────────────────▼------------------┐
│                    Baran Runtime (Node B)                     │
│  ...                                                          │
└──────────────────────────────────────────────────────────────┘
```

## Wildfire Federation Example

See [`examples/wildfire/README.md`](../../examples/wildfire/README.md#multi-node-federation-example) for a step-by-step walkthrough of running the wildfire example across two federated nodes, with fire detection agents on Node A and the evacuation planner on Node B.

## Limitations (v1)

- Maximum ~10 federated nodes (v1 is designed for small clusters in the same datacenter)
- Pre-shared key authentication only (no TLS certificates in v1)
- Same datacenter/region network assumed (high-latency relay not optimized)
- No saga/compensation — relay failures propagate as workflow step failures (fail-fast)
