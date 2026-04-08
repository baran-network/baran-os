# hello-baran

The minimal Baran OS evaluation demo. One command, two agents, two
capabilities, one workflow — designed so you can see Baran alive in under two
minutes on a clean machine.

## What it does

The demo runs a deterministic two-step workflow that transforms the constant
input string `"hello baran"`:

| Step | Capability        | Agent (language)              | Transform                      |
| ---- | ----------------- | ----------------------------- | ------------------------------ |
| 1    | `text.uppercase`  | `uppercaser` (Go, native)     | `{"text": s}` → upper-cased    |
| 2    | `text.reverse`    | `reverser` (Python, sidecar)  | `{"text": s}` → reversed       |

The two agents are heterogeneous on purpose: one speaks the Baran event bus
natively via the Go SDK, the other speaks JSON over HTTP/SSE through the
**sidecar gateway** (spec 014). The runtime dispatches each step **by
declared capability** — never by agent identity — which is the central idea of
the Baran scheduler.

## Run it

Prerequisites: Docker (with the `docker compose` v2 plugin) and GNU Make.
Nothing else — no Go toolchain, no Python, no Node, no API keys, no network
access after the first image build.

```sh
make demo            # build images, boot the stack, print the UI URL
open http://localhost:3000   # operator UI
make demo-down       # tear everything down (containers, network, volume)
```

On the first run, image builds add ~1–2 minutes. Subsequent runs reuse the
cache and reach a completed workflow in well under two minutes.

## What to look for in the operator UI

Open <http://localhost:3000> and you should see, within a few seconds of
`make demo`:

1. **Two registered agents** with distinct capabilities
   (`uppercaser` → `text.uppercase`, `reverser` → `text.reverse`).
2. **One workflow** named `hello-baran` in `COMPLETED` state.
3. **Two step events** dispatched to **two distinct agent IDs** — one Go,
   one Python.

That's the proof Baran is doing its job: capability-based dispatch across a
heterogeneous fleet, with the operator UI as the read-only observation
surface.

## Files

```text
examples/hello-baran/
├── README.md                       ← you are here
├── workflow/hello.json             ← declarative 2-step workflow
├── agents/uppercaser/main.go       ← Go native agent (capability text.uppercase)
├── agents/reverser/main.py         ← Python agent via sidecar (text.reverse)
└── trigger/main.go                 ← one-shot seeder (publishes workflow.start)

deploy/demo/                        ← Compose stack + per-service Dockerfiles
scripts/demo/                       ← up.sh / down.sh / smoke.sh
```

## CI smoke test

The same stack is exercised in CI on every PR that touches the runtime,
sidecar, SDKs, UI, or this example:

```sh
make demo-smoke      # boots, asserts COMPLETED + 2 distinct agents, tears down
```

The smoke test polls the runtime's `/api/workflows` endpoint, asserts a
workflow reaches `COMPLETED` with exactly two steps on two distinct agent
IDs, and **always** runs `make demo-down` on exit (success or failure).

## Looking for more?

Once `hello-baran` makes sense, the larger end-to-end examples live in
`examples/wildfire/` (multi-agent emergency response with human-in-loop) and
`examples/coding/` (autonomous LangGraph coding agent).
