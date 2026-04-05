"""Integration test: full 3-step coding workflow end-to-end.

Requires:
- Running Baran runtime + sidecar (make dev)
- BARAN_PSK, SIDECAR_URL env vars set
- LLM API key (ANTHROPIC_API_KEY or OPENAI_API_KEY)

Run with: pytest tests/test_workflow.py -m integration
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import config
from models import CodingTask, TaskAnalysis, GeneratedCode, ReviewFeedback


def _has_all_prereqs() -> bool:
    return bool(
        config.BARAN_PSK
        and (config.ANTHROPIC_API_KEY or config.OPENAI_API_KEY)
    )


pytestmark = pytest.mark.skipif(
    not _has_all_prereqs(),
    reason="Requires BARAN_PSK + LLM API key + running Baran sidecar",
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_analyst_agent_registers_and_subscribes():
    """Analyst agent can register with sidecar and start event loop."""
    from baran import BaranAgent, Capability

    agent = BaranAgent(
        name="test-workflow-analyst",
        agent_type="llm",
        version="1.0.0",
        token=config.BARAN_PSK,
        sidecar_url=config.SIDECAR_URL,
        capabilities=[Capability(name="code.analysis", version="v1.0")],
    )
    await agent.start()
    assert agent.agent_id is not None
    await agent.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_workflow_end_to_end():
    """Submit a coding task and verify all 3 steps complete with correct payloads.

    This test starts all 3 agents in background tasks, then uses the Go trigger
    via subprocess to submit the workflow. Results are verified by agent handlers.
    """
    from baran import BaranAgent, Capability
    from baran.events import Event

    step_results: dict[str, dict] = {}
    done_event = asyncio.Event()

    analyst = BaranAgent(
        name="test-e2e-analyst",
        agent_type="llm",
        version="1.0.0",
        token=config.BARAN_PSK,
        sidecar_url=config.SIDECAR_URL,
        capabilities=[Capability(name="code.analysis", version="v1.0")],
    )

    generator = BaranAgent(
        name="test-e2e-generator",
        agent_type="llm",
        version="1.0.0",
        token=config.BARAN_PSK,
        sidecar_url=config.SIDECAR_URL,
        capabilities=[Capability(name="code.generation", version="v1.0")],
    )

    reviewer = BaranAgent(
        name="test-e2e-reviewer",
        agent_type="llm",
        version="1.0.0",
        token=config.BARAN_PSK,
        sidecar_url=config.SIDECAR_URL,
        capabilities=[Capability(name="code.review", version="v1.0")],
    )

    @analyst.on("workflow.step")
    async def handle_analysis(event: Event) -> bytes | None:
        from agents.analyst import _call_llm, _load_prompt, _parse_analysis
        task = CodingTask.from_dict(event.payload)
        prompt = _load_prompt()
        raw = await asyncio.get_event_loop().run_in_executor(None, _call_llm, prompt, task.description)
        analysis = _parse_analysis(raw, task)
        await analyst.publish("workflow.step", analysis.to_dict(), workflow_id=event.workflow_id)
        step_results["analysis"] = analysis.to_dict()
        return b"ok"

    @generator.on("workflow.step")
    async def handle_generation(event: Event) -> bytes | None:
        from agents.generator import _call_llm_direct, _load_prompt, _parse_code
        analysis = TaskAnalysis.from_dict(event.payload)
        prompt = _load_prompt()
        raw = await asyncio.get_event_loop().run_in_executor(
            None, _call_llm_direct, prompt, json.dumps(analysis.to_dict())
        )
        generated = _parse_code(raw, analysis, [])
        await generator.publish("workflow.step", generated.to_dict(), workflow_id=event.workflow_id)
        step_results["generation"] = generated.to_dict()
        return b"ok"

    @reviewer.on("workflow.step")
    async def handle_review(event: Event) -> bytes | None:
        from agents.reviewer import _call_llm, _load_prompt, _parse_feedback
        generated = GeneratedCode.from_dict(event.payload)
        prompt = _load_prompt()
        msg = f"Review:\n{generated.code}"
        raw = await asyncio.get_event_loop().run_in_executor(None, _call_llm, prompt, msg)
        feedback = _parse_feedback(raw, generated)
        await reviewer.publish("workflow.step", feedback.to_dict(), workflow_id=event.workflow_id)
        step_results["review"] = feedback.to_dict()
        done_event.set()
        return b"ok"

    await asyncio.gather(analyst.start(), generator.start(), reviewer.start())

    try:
        # Run all event loops in parallel.
        tasks = [
            asyncio.create_task(analyst._event_loop()),
            asyncio.create_task(generator._event_loop()),
            asyncio.create_task(reviewer._event_loop()),
        ]

        # Small delay to let subscriptions initialize.
        await asyncio.sleep(0.5)

        # Submit workflow via trigger subprocess.
        trigger_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "trigger")
        proc = subprocess.Popen(
            ["go", "run", ".", "Implement a function that checks if a number is prime"],
            cwd=trigger_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Wait up to 90 seconds for workflow to complete.
        try:
            await asyncio.wait_for(done_event.wait(), timeout=90.0)
        except asyncio.TimeoutError:
            proc.terminate()
            pytest.fail("Workflow did not complete within 90 seconds")

        proc.wait(timeout=5)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(analyst.stop(), generator.stop(), reviewer.stop())

    assert "analysis" in step_results, "Analyst step must complete"
    assert "generation" in step_results, "Generator step must complete"
    assert "review" in step_results, "Reviewer step must complete"

    analysis = TaskAnalysis.from_dict(step_results["analysis"])
    assert len(analysis.requirements) > 0

    generated = GeneratedCode.from_dict(step_results["generation"])
    assert len(generated.code) > 10

    review = ReviewFeedback.from_dict(step_results["review"])
    assert review.verdict in ("approve", "request_changes")
    assert 1 <= review.quality_score <= 10
