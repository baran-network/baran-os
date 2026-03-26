# Sidecar Gateway

The Sidecar Gateway enables agents written in **any language** to participate in the Baran OS network via HTTP. It translates between HTTP/JSON and the native NATS/protobuf protocol, so external agents need zero knowledge of NATS or protobuf.

## How It Works

```
┌──────────────┐   HTTP/JSON    ┌──────────────┐  NATS/protobuf  ┌──────────────┐
│  External    │ ◄────────────► │   Sidecar    │ ◄─────────────► │  Baran OS    │
│  Agent       │  REST + SSE    │   Gateway    │    Go SDK        │  Runtime     │
│  (Python/TS) │  or WebSocket  │              │                  │              │
└──────────────┘                └──────────────┘                  └──────────────┘
```

The sidecar runs as a separate process alongside the Baran OS runtime. It:

1. **Registers** external agents into the network (they become discoverable, receive health pings)
2. **Translates** JSON payloads to/from protobuf using the protocol definitions as source of truth
3. **Routes** events through the standard event router — external agents are indistinguishable from native Go agents
4. **Streams** events to external agents via SSE or WebSocket

## Quick Start

### 1. Start the Sidecar

```bash
baran-sidecar --nats-url nats://localhost:4222 --psk my-secret-key
```

The sidecar starts on port 9090 by default. The `--psk` flag sets the pre-shared key used for authentication.

### 2. Register an Agent

```bash
curl -X POST http://localhost:9090/agents \
  -H "Authorization: Bearer my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-agent",
    "agent_type": "analyzer",
    "version": "1.0.0",
    "capabilities": [
      {"name": "echo.text", "version": "1.0.0", "description": "Echoes text"}
    ]
  }'
```

Response:

```json
{"agent_id": "01966abc-...", "status": "active", "registered_at": "2026-03-25T10:00:00Z"}
```

### 3. Subscribe to Events (SSE)

```bash
curl -N http://localhost:9090/agents/{agent_id}/events \
  -H "Authorization: Bearer my-secret-key" \
  -H "Accept: text/event-stream"
```

Events arrive as standard SSE messages:

```
event: workflow.step
id: 01966abc-def0-7000-8000-000000000010
data: {"source_agent":"orchestrator-001","workflow_id":"...","payload":{...}}
```

### 4. Publish an Event

```bash
curl -X POST http://localhost:9090/agents/{agent_id}/events \
  -H "Authorization: Bearer my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "workflow.step.result",
    "workflow_id": "wf-123",
    "payload": {
      "step_index": 1,
      "status": "SUCCESS",
      "result": "aGVsbG8gd29ybGQ="
    }
  }'
```

### 5. Acknowledge Events

Events delivered via SSE are not auto-acknowledged. Acknowledge them to advance the consumer position:

```bash
curl -X POST http://localhost:9090/agents/{agent_id}/ack \
  -H "Authorization: Bearer my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"event_id": "01966abc-def0-7000-8000-000000000010"}'
```

### 6. Deregister

```bash
curl -X DELETE http://localhost:9090/agents/{agent_id} \
  -H "Authorization: Bearer my-secret-key"
```

## REST API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/agents` | Register an external agent |
| `DELETE` | `/agents/{id}` | Deregister an agent |
| `POST` | `/agents/{id}/events` | Publish an event |
| `GET` | `/agents/{id}/events` | Subscribe via SSE |
| `POST` | `/agents/{id}/ack` | Acknowledge an event |
| `GET` | `/agents/{id}/ws` | WebSocket connection |
| `GET` | `/health` | Sidecar health status |

All endpoints (except `/health`) require authentication via `Authorization: Bearer <psk>` header or `?token=<psk>` query parameter.

## SSE Protocol

The SSE stream follows the standard [Server-Sent Events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events) specification:

- **`event`**: The event type (e.g., `workflow.step`, `agent.direct`)
- **`id`**: The event UUID, used for acknowledgment and reconnection
- **`data`**: JSON payload with the event envelope (source, workflow, correlation, payload)
- **Keepalive**: A `: keepalive` comment every 15 seconds
- **Reconnection**: Send `Last-Event-ID` header to resume from where you left off

## WebSocket Protocol

For lower latency and full-duplex communication, connect via WebSocket at `GET /agents/{id}/ws`.

**Server → Client** (event delivery):

```json
{
  "type": "event",
  "event_type": "workflow.step",
  "event_id": "01966abc-...",
  "payload": { ... }
}
```

**Client → Server** (publish):

```json
{
  "type": "publish",
  "event_type": "workflow.step.result",
  "payload": { ... }
}
```

**Client → Server** (acknowledge):

```json
{
  "type": "ack",
  "event_id": "01966abc-..."
}
```

WebSocket supports reconnection via `?last_event_id=<id>` query parameter and uses custom close codes (4001 unauthorized, 4004 agent not found, 4008 timeout).

## Python SDK

```bash
pip install baran-sdk
```

```python
import asyncio
from baran import BaranAgent, Capability

agent = BaranAgent(
    name="echo-agent",
    agent_type="echo",
    version="1.0.0",
    token="my-secret-key",
    capabilities=[Capability(name="echo.text", version="1.0.0")],
)

@agent.on("workflow.step")
async def handle_step(event):
    text = event.payload["step"]["parameters"].get("text", "")
    return text.encode()

asyncio.run(agent.run())
```

The Python SDK handles registration, SSE subscription, event acknowledgment, and graceful shutdown. See the [Python SDK API reference](https://github.com/baran-network/baran-os/tree/main/sdks/python) for full details.

## TypeScript SDK

```bash
npm install @baran/sdk
```

```typescript
import { BaranAgent } from '@baran/sdk';

const agent = new BaranAgent({
  name: 'echo-agent',
  agentType: 'echo',
  version: '1.0.0',
  token: 'my-secret-key',
  capabilities: [{ name: 'echo.text', version: '1.0.0' }],
});

agent.on('workflow.step', async (event) => {
  const text = event.payload.step?.parameters?.text ?? '';
  return Buffer.from(text);
});

agent.run();
```

The TypeScript SDK provides typed events, async/await handlers, and automatic lifecycle management. See the [TypeScript SDK API reference](https://github.com/baran-network/baran-os/tree/main/sdks/typescript) for full details.

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `BARAN_SIDECAR_PORT` | `9090` | HTTP server port |
| `--nats-url` | `BARAN_SIDECAR_NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `--psk` | `BARAN_SIDECAR_PSK` | (required) | Pre-shared key for authentication |
| `--log-level` | `BARAN_SIDECAR_LOG_LEVEL` | `info` | Log verbosity |
| `--max-agents` | `BARAN_SIDECAR_MAX_AGENTS` | `50` | Max concurrent agents per sidecar |

Configuration follows flag > environment variable > default precedence.

## Error Handling

All errors use a consistent JSON format:

```json
{
  "error": "human-readable error message",
  "code": "AGENT_NOT_FOUND",
  "details": {}
}
```

Error codes: `UNAUTHORIZED`, `AGENT_NOT_FOUND`, `AGENT_CONFLICT`, `INVALID_REQUEST`, `INVALID_PAYLOAD`, `UNKNOWN_EVENT_TYPE`, `SERVICE_UNAVAILABLE`.
