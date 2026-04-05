# LLM Coding Example

A complete end-to-end example demonstrating how Python LLM agents connect to Baran OS through the Sidecar Gateway and collaborate in a multi-step workflow to autonomously analyze, generate, and review code.

## What This Example Demonstrates

- **Sidecar Gateway integration** — Python agents connect to Baran OS via HTTP/SSE without any NATS or protobuf dependency
- **Multi-agent workflow** — three specialized agents execute sequentially, each building on the previous result
- **LLM integration** — agents use Anthropic Claude or OpenAI GPT-4o via direct SDK calls
- **MCP tool access** — the generator agent uses LangGraph + MCP filesystem tools during code generation, with graceful fallback if MCP is unavailable
- **Capability-based dispatch** — the workflow engine selects agents by capability (`code.analysis`, `code.generation`, `code.review`), not by identity

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

The Go trigger connects directly to NATS to submit a `workflow.start` event. The three Python agents connect through the Sidecar Gateway — they register, subscribe to events via SSE, and publish results back through HTTP. They have no direct NATS or protobuf dependency.

## Agents

| Agent | File | Capability | Input | Output |
|-------|------|------------|-------|--------|
| Task Analyst | `agents/analyst.py` | `code.analysis` | `CodingTask` | `TaskAnalysis` |
| Code Generator | `agents/generator.py` | `code.generation` | `TaskAnalysis` | `GeneratedCode` |
| Code Reviewer | `agents/reviewer.py` | `code.review` | `GeneratedCode` | `ReviewFeedback` |

## Prerequisites

- Go 1.23+ (for Baran runtime, sidecar, and trigger)
- Python 3.10+ with pip
- An LLM API key (Anthropic or OpenAI)
- Node.js / npx (optional — only needed for MCP filesystem tools)

## Setup

### 1. Build and start the Baran runtime

```bash
# From the repo root
make dev
```

This builds and starts the runtime with an embedded NATS server on `:4222`.

### 2. Build and start the Sidecar Gateway

```bash
# From the repo root (in a new terminal)
go build -o baran-sidecar ./sidecar/cmd/baran-sidecar
./baran-sidecar --psk dev-token
```

The sidecar starts on port 9090 by default.

### 3. Install Python agent dependencies

```bash
cd examples/coding
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
export LLM_PROVIDER=anthropic          # or "openai"
export ANTHROPIC_API_KEY=sk-ant-...    # or OPENAI_API_KEY=sk-...
export BARAN_PSK=dev-token             # must match --psk above
```

## Running the Example

Start each component in its own terminal from the `examples/coding/` directory:

```bash
# Terminal 1 — task analyst agent
python agents/analyst.py

# Terminal 2 — code generator agent
python agents/generator.py

# Terminal 3 — code reviewer agent
python agents/reviewer.py

# Terminal 4 — submit a coding task
go run trigger/main.go "Implement a function that checks if a string is a palindrome, with tests"
```

Each agent logs registration, event receipt, and result publishing. Wait for all three to print "registered" before running the trigger.

### Trigger flags

```
go run trigger/main.go [flags] "<task description>"

Flags:
  -language string   Target programming language (default "python")
  -nats-url string   NATS server URL (default "nats://localhost:4222")
  -timeout duration  Workflow timeout (default 2m0s)
```

## Expected Output

```
=== Coding Workflow Started ===
Task: Implement a function that checks if a string is a palindrome, with tests
Language: python
Waiting for completion...

=== Coding Workflow Result ===

Workflow ID: 01966abc-...
Duration: 42.3s

--- Analysis ---
Requirements: [check palindrome, handle edge cases, include unit tests]
Constraints:  [case-insensitive, handle empty string]
Approach:     Two-pointer comparison with normalization
Complexity:   low

--- Generated Code ---
Language: python
Tools used: [read_file]
Explanation: Complete palindrome implementation with pytest test suite

def is_palindrome(s: str) -> bool:
    ...

--- Review ---
Verdict: approve
Quality: 9/10
Suggestions: [Consider Unicode normalization for internationalization]
```

## Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_PROVIDER` | `anthropic` | LLM provider: `anthropic` or `openai` |
| `ANTHROPIC_API_KEY` | — | Anthropic API key |
| `OPENAI_API_KEY` | — | OpenAI API key |
| `BARAN_PSK` | — | Sidecar pre-shared key (must match `--psk`) |
| `SIDECAR_URL` | `http://localhost:9090` | Sidecar gateway URL |
| `MCP_SERVER_CMD` | `npx @modelcontextprotocol/server-filesystem .` | MCP server command for the generator agent |

## MCP Tool Access (Generator Agent)

The generator agent uses LangGraph with an MCP filesystem server to read project files during code generation. If the MCP server is unavailable (e.g., Node.js not installed), the agent falls back to a direct LLM call and logs a warning — the workflow continues without interruption.

To disable MCP intentionally:

```bash
export MCP_SERVER_CMD=""
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `Agent not started` / registration timeout | Sidecar not running or wrong `BARAN_PSK` | Start sidecar with matching `--psk` |
| `No agent found for capability` | Agents not yet registered | Wait for all three agents to log "registered" |
| LLM timeout or auth error | Invalid or missing API key | Check `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` |
| Workflow timeout | LLM taking too long | Use `-timeout 3m` flag on the trigger |
| MCP warning in generator logs | Node.js / npx not installed | Install Node.js or set `MCP_SERVER_CMD=""` |

## File Structure

```
examples/coding/
├── agents/
│   ├── analyst.py      # Task analyst (code.analysis capability)
│   ├── generator.py    # Code generator (code.generation, LangGraph + MCP)
│   └── reviewer.py     # Code reviewer (code.review capability)
├── prompts/
│   ├── analyst.txt     # System prompt for analyst
│   ├── generator.txt   # System prompt for generator (includes MCP instructions)
│   └── reviewer.txt    # System prompt for reviewer
├── trigger/
│   └── main.go         # Go trigger — submits workflow, displays result
├── tests/
│   ├── test_agent_connectivity.py  # Sidecar connectivity tests
│   ├── test_analyst.py             # Analyst prompt/output tests
│   ├── test_generator.py           # Generator tests (+ MCP tool invocation)
│   ├── test_reviewer.py            # Reviewer prompt/output tests
│   └── test_workflow.py            # Full end-to-end workflow test
├── config.py           # Shared configuration (env vars)
├── models.py           # Domain payload dataclasses
└── requirements.txt    # Python dependencies
```
