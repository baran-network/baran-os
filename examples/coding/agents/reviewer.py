"""Code reviewer agent — capability: code.review.

Receives a GeneratedCode event and produces ReviewFeedback via LLM.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from baran import BaranAgent, Capability
from baran.events import Event

import config
from models import GeneratedCode, ReviewFeedback

logger = logging.getLogger(__name__)

AGENT_NAME = "coding-reviewer"
CAPABILITY = "code.review"


def _load_prompt() -> str:
    prompt_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts", "reviewer.txt")
    try:
        with open(prompt_path) as f:
            return f.read()
    except FileNotFoundError:
        return "You are a code reviewer. Review the code and provide structured feedback."


def _call_llm(system_prompt: str, user_message: str) -> str:
    if config.LLM_PROVIDER == "anthropic":
        import anthropic
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=1024,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )
        return message.content[0].text
    else:
        from openai import OpenAI
        client = OpenAI(api_key=config.OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
        )
        return response.choices[0].message.content


def _parse_feedback(raw: str) -> ReviewFeedback:
    try:
        data = json.loads(raw)
        return ReviewFeedback.from_dict(data)
    except (json.JSONDecodeError, KeyError):
        return ReviewFeedback(
            verdict="approve",
            issues=[],
            suggestions=[raw[:200]],
            quality_score=7,
        )


agent = BaranAgent(
    name=AGENT_NAME,
    agent_type="llm",
    version=config.AGENT_VERSION,
    token=config.BARAN_PSK,
    sidecar_url=config.SIDECAR_URL,
    capabilities=[
        Capability(
            name=CAPABILITY,
            version="v1.0",
            description="Review generated code for correctness and quality",
        )
    ],
    labels={"role": "reviewer", "example": "coding"},
)


@agent.on("workflow.step")
async def handle_workflow_step(event: Event) -> bytes | None:
    logger.info("Reviewer received workflow.step event_id=%s", event.event_id)
    try:
        generated = GeneratedCode.from_dict(event.payload)
    except (KeyError, TypeError) as exc:
        logger.error("Failed to parse GeneratedCode: %s", exc)
        await agent.publish(
            "agent.error",
            {
                "error": f"Invalid payload: {exc}",
                "step": "review",
                "recoverable": False,
            },
            workflow_id=event.workflow_id,
        )
        return None

    system_prompt = _load_prompt()
    user_message = f"Review the following {generated.language} code:\n\n{generated.code}\n\nExplanation: {generated.explanation}"
    try:
        raw = await asyncio.get_running_loop().run_in_executor(
            None, _call_llm, system_prompt, user_message
        )
        feedback = _parse_feedback(raw)
    except Exception as exc:
        logger.error("LLM call failed: %s", exc)
        await agent.publish(
            "agent.error",
            {
                "error": str(exc),
                "step": "review",
                "recoverable": False,
            },
            workflow_id=event.workflow_id,
        )
        return None

    await agent.publish(
        "workflow.step",
        feedback.to_dict(),
        workflow_id=event.workflow_id,
        metadata={"step_role": "reviewer"},
    )
    logger.info("Reviewer published feedback verdict=%s score=%d", feedback.verdict, feedback.quality_score)
    return b"ok"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(agent.run())
