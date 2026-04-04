# A2A Gateway

The A2A Gateway bridges [Google's Agent-to-Agent (A2A) protocol](https://google.github.io/A2A/) to the Baran OS network. It works in two directions:

- **Outbound**: Exposes Baran agents as A2A Agent Cards so external A2A clients can discover and invoke them.
- **Inbound**: Onboards external A2A agents into Baran as virtual participants so Baran workflows can dispatch to them.

```
┌──────────────┐  A2A/JSON-RPC  ┌──────────────┐  NATS/protobuf  ┌──────────────┐
│  External    │ ◄────────────► │  baran-a2a   │ ◄─────────────► │  Baran OS    │
│  A2A Client  │  HTTP + REST   │  Gateway     │    Go SDK        │  Runtime     │
└──────────────┘                └──────────────┘                  └──────────────┘
```

The gateway is stateless — all state lives in JetStream KV via the Go SDK.

## Quick Start

### 1. Start the Gateway

```bash
baran-a2a --nats-url nats://localhost:4222 --a2a-port 8090 --psk my-secret
```

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--nats-url` | `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `--a2a-port` | `A2A_PORT` | `8090` | HTTP port for the gateway |
| `--psk` | `A2A_PSK` | — | Pre-shared key for `Authorization: Bearer` auth |
| `--config` | `A2A_CONFIG` | — | Path to YAML config for external agents |

### 2. Discover Baran Agents

```bash
curl http://localhost:8090/.well-known/agent-card.json
```

Returns a composite Agent Card aggregating all active Baran agents as A2A skills:

```json
{
  "name": "Baran OS Node",
  "description": "Baran OS agent network",
  "version": "1.0.0",
  "skills": [
    {
      "id": "nlp.summarization",
      "name": "NLP Summarization",
      "description": "Summarize text content",
      "tags": ["nlp", "nlp.summarization"],
      "input_modes": ["text/plain"],
      "output_modes": ["text/plain"]
    }
  ]
}
```

### 3. Send a Task

All A2A operations use JSON-RPC 2.0 on `POST /`:

```bash
curl -X POST http://localhost:8090/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer my-secret" \
  -d '{
    "jsonrpc": "2.0",
    "method": "message/send",
    "id": "1",
    "params": {
      "message": {
        "message_id": "msg-1",
        "role": "user",
        "parts": [{"text": "Summarize this article..."}]
      },
      "configuration": {
        "skill": "nlp.summarization"
      }
    }
  }'
```

Response:

```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "result": {
    "id": "task-uuid",
    "status": {
      "state": "TASK_STATE_SUBMITTED",
      "updatedAt": "2026-03-28T10:00:00Z"
    }
  }
}
```

### 4. Poll for Results

```bash
curl -X POST http://localhost:8090/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer my-secret" \
  -d '{"jsonrpc":"2.0","method":"tasks/get","id":"2","params":{"id":"task-uuid"}}'
```

## API Reference

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/.well-known/agent-card.json` | A2A Agent Card discovery (no auth required) |
| `POST` | `/` | JSON-RPC 2.0 dispatch (auth required) |

### JSON-RPC Methods

| Method | Description |
|--------|-------------|
| `message/send` | Submit a task to a Baran agent by skill |
| `tasks/get` | Get task status and artifacts |
| `tasks/cancel` | Cancel an in-progress task |

### Task State Mapping

| Baran Workflow State | A2A Task State |
|---------------------|----------------|
| CREATED | TASK_STATE_SUBMITTED |
| RUNNING | TASK_STATE_WORKING |
| COMPLETED | TASK_STATE_COMPLETED |
| FAILED | TASK_STATE_FAILED |
| WAITING_HUMAN | TASK_STATE_INPUT_REQUIRED |

### Error Codes

| Error | Code | When |
|-------|------|------|
| `TaskNotFoundError` | -32001 | Task ID doesn't match any workflow |
| `TaskNotCancelableError` | -32002 | Workflow already in terminal state |
| `UnsupportedOperationError` | -32003 | Method not implemented |
| `ContentTypeNotSupportedError` | -32004 | Input mode not accepted by agent |
| `SkillNotFoundError` | -32005 | No agent matches requested skill |

## Onboarding External A2A Agents

External A2A agents can be onboarded into Baran so that Baran workflows can dispatch to them transparently.

### Configuration

Create a YAML config file and pass it via `--config`:

```yaml
external_agents:
  - name: "external-coder"
    endpoint: "https://external-agent.example.com"
    poll_interval: 30s
    skills_mapping:             # Optional: explicit skill → capability mapping
      "gen.code": "code.generation"
```

### How Onboarding Works

1. Gateway fetches `{endpoint}/.well-known/agent-card.json`
2. For each skill in the Agent Card:
   - **Standard match**: skill ID matches a taxonomy entry → registered directly
   - **Alias match**: alias registry has an equivalent → use resolved name
   - **No match**: auto-registered as `a2a.{agent_name}.{skill_id}` (vendor capability)
3. Agent registered in Baran registry with `origin: "a2a"`
4. Health polling starts — re-fetches Agent Card at `poll_interval`; marks agent UNHEALTHY on HTTP error

### Dispatch to External Agents

When a Baran workflow dispatches a step to a virtual A2A agent, the gateway:

1. Detects `origin: "a2a"` on the matched agent
2. Reads the `a2a_endpoint` from agent parameters
3. Translates the Baran `WorkflowStepPayload` to a `message/send` A2A request
4. POSTs to the external agent's endpoint
5. Polls `tasks/get` until terminal state
6. Translates the A2A response back to a Baran `WorkflowStepResult`

The external agent is indistinguishable from a local Baran agent from the workflow engine's perspective.
