# LLM Agent Example

This example demonstrates how Python LLM agents connect to Baran OS through the [Sidecar Gateway](sidecar-gateway.md) and collaborate in a multi-step coding workflow: task analysis → code generation → code review.

It is the reference implementation for using Baran OS with external LLM-powered agents, combining:

- **Sidecar Gateway** for language-agnostic agent connectivity
- **Anthropic Claude or OpenAI GPT-4o** as the LLM backbone
- **LangGraph + MCP** for tool-augmented code generation
- **Capability-based dispatch** to route workflow steps to specialized agents

## Architecture

```
User → [Go Trigger] → Baran Runtime (workflow engine)
                            │
         ┌──────────────────┼──────────────────┐
         ▼                  ▼                   ▼
   Step 1: analysis    Step 2: generation  Step 3: review
   (code.analysis)     (code.generation)   (code.review)
         │                  │                   │
         ▼                  ▼                   ▼
   [Analyst Agent]    [Generator Agent]   [Reviewer Agent]
   (Python/LLM)      (Python/LangGraph   (Python/LLM)
                       + MCP tools)
         │                  │                   │
         └──────────────────┴───────────────────┘
                   via Sidecar Gateway (HTTP/SSE)
```

The Go trigger publishes a `workflow.start` event directly to NATS. The three Python agents connect to the Sidecar Gateway via HTTP — they have no dependency on NATS or protobuf.

## Quick Start

### Prerequisites

- Go 1.23+, Python 3.10+, pip
- Anthropic or OpenAI API key
- Node.js (optional, for MCP filesystem tools)

### 1. Start the runtime and sidecar

```bash
# Terminal 1: Baran runtime
make dev

# Terminal 2: Sidecar Gateway
go build -o baran-sidecar ./sidecar/cmd/baran-sidecar
./baran-sidecar --psk dev-token
```

### 2. Install Python dependencies

```bash
cd examples/coding
pip install -r requirements.txt
```

### 3. Configure and run

```bash
export LLM_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...
export BARAN_PSK=dev-token

# Terminal 3
python agents/analyst.py

# Terminal 4
python agents/generator.py

# Terminal 5
python agents/reviewer.py

# Terminal 6 — submit a task
go run trigger/main.go "Implement a palindrome checker with unit tests"
```

### Expected output

```
=== Coding Workflow Result ===

Workflow ID: 01966abc-...
Duration: 42.3s

--- Analysis ---
Requirements: [check palindrome, handle edge cases, include unit tests]
Approach:     Two-pointer comparison with normalization
Complexity:   low

--- Generated Code ---
Language: python
Tools used: [read_file]

def is_palindrome(s: str) -> bool:
    ...

--- Review ---
Verdict: approve
Quality: 9/10
Suggestions: [Consider Unicode normalization]
```

## Agent Design

Each agent follows the same pattern:

1. **Register** with the sidecar, announcing one capability (`code.analysis`, `code.generation`, or `code.review`)
2. **Subscribe** to `workflow.step` events via SSE
3. **Process** the incoming payload with an LLM call
4. **Publish** the result back through the sidecar

```python
from baran import BaranAgent, Capability

agent = BaranAgent(
    name="coding-analyst",
    agent_type="llm",
    version="1.0.0",
    token=config.BARAN_PSK,
    sidecar_url=config.SIDECAR_URL,
    capabilities=[Capability(name="code.analysis", version="v1.0", description="...")],
)

@agent.on("workflow.step")
async def handle(event):
    task = CodingTask.from_dict(event.payload)
    analysis = call_llm(task)
    await agent.publish("workflow.step", analysis.to_dict(), workflow_id=event.workflow_id)

asyncio.run(agent.run())
```

## MCP Tool Integration

The generator agent uses [LangGraph](https://langchain-ai.github.io/langgraph/) with an MCP filesystem server, allowing the LLM to read project files during code generation. If MCP is unavailable, the agent falls back to a direct LLM call automatically.

```
Generator Agent
│
├── LangGraph ReAct loop
│   ├── LLM decides which tools to call
│   ├── [MCP tool: read_file] → reads existing code
│   ├── [MCP tool: list_directory] → explores project
│   └── LLM generates final code
│
└── Falls back to direct LLM call if MCP server unavailable
```

To disable MCP:

```bash
export MCP_SERVER_CMD=""
```

## Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_PROVIDER` | `anthropic` | LLM provider: `anthropic` or `openai` |
| `ANTHROPIC_API_KEY` | — | Anthropic API key |
| `OPENAI_API_KEY` | — | OpenAI API key |
| `BARAN_PSK` | — | Sidecar pre-shared key (must match `--psk`) |
| `SIDECAR_URL` | `http://localhost:9090` | Sidecar gateway URL |
| `MCP_SERVER_CMD` | `npx @modelcontextprotocol/server-filesystem .` | MCP server command |

## Full Example

The complete example is in [`examples/coding/`](https://github.com/baran-network/baran-os/tree/main/examples/coding) — see the [README](https://github.com/baran-network/baran-os/tree/main/examples/coding/README.md) for the full file structure and troubleshooting guide.
