"""hello-baran reverser agent — Python agent via the Baran sidecar gateway.

Capability: text.reverse — pure transform {"text": s} -> {"text": s[::-1]}.

The agent registers with the sidecar (Phase 8 / spec 014), declares the
``text.reverse`` capability, and handles ``workflow.step`` events for that
capability. The transform is stateless and offline (no I/O, no LLM).

Note on the Phase 9 capability taxonomy (spec 015): ``text`` is not one of the
8 standard top-level categories, so ``text.reverse`` is accepted as a valid
vendor capability — no alias registration is required.
"""

from __future__ import annotations

import asyncio
import json
import os

from baran import BaranAgent, Capability, Event


async def main() -> None:
    sidecar_url = os.environ.get("BARAN_SIDECAR_URL", "http://localhost:9090")
    token = os.environ.get("BARAN_SIDECAR_TOKEN", "")

    agent = BaranAgent(
        name="reverser",
        agent_type="hello-baran-agent",
        version="0.1.0",
        token=token,
        sidecar_url=sidecar_url,
        capabilities=[
            Capability(
                name="text.reverse",
                version="0.1.0",
                description="Returns the input text reversed",
            )
        ],
    )

    @agent.on("workflow.step")
    async def handle(event: Event) -> bytes | None:
        payload = event.payload or {}
        # The sidecar exposes the step input under "input" (raw JSON object).
        step_input = payload.get("input") or payload.get("step", {}).get("input") or {}
        if isinstance(step_input, str):
            try:
                step_input = json.loads(step_input)
            except Exception:
                step_input = {"text": step_input}
        text = (step_input or {}).get("text", "")
        result = {"text": text[::-1]}
        return json.dumps(result).encode("utf-8")

    await agent.run()


if __name__ == "__main__":
    asyncio.run(main())
