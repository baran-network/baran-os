"""Task analyst agent — capability: code.analysis.

Receives a CodingTask event, produces a structured TaskAnalysis via LLM,
and publishes the result back through the sidecar gateway.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from baran import BaranAgent, Capability
from baran.events import Event

import config
from models import CodingTask, TaskAnalysis

logger = logging.getLogger(__name__)

AGENT_NAME = "coding-analyst"
CAPABILITY = "code.analysis"


def _load_prompt() -> str:
    prompt_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts", "analyst.txt")
    try:
        with open(prompt_path) as f:
            return f.read()
    except FileNotFoundError:
        return "You are a task analyst. Analyze the coding task and produce structured requirements."


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


def _parse_analysis(raw: str, task: CodingTask) -> TaskAnalysis:
    try:
        data = json.loads(raw)
        return TaskAnalysis.from_dict(data)
    except (json.JSONDecodeError, KeyError):
        return TaskAnalysis(
            requirements=[task.description],
            constraints=[],
            approach=raw[:200],
            language=task.language,
            complexity="medium",
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
            description="Analyze coding tasks and produce structured requirements",
        )
    ],
    labels={"role": "analyst", "example": "coding"},
)


@agent.on("workflow.step")
async def handle_workflow_step(event: Event) -> bytes | None:
    logger.info("Analyst received workflow.step event_id=%s", event.event_id)
    try:
        task = CodingTask.from_dict(event.payload)
    except (KeyError, TypeError) as exc:
        logger.error("Failed to parse CodingTask: %s", exc)
        await agent.publish(
            "agent.error",
            {
                "error": f"Invalid payload: {exc}",
                "step": "analysis",
                "recoverable": False,
            },
            workflow_id=event.workflow_id,
        )
        return None

    system_prompt = _load_prompt()
    try:
        raw = await asyncio.get_running_loop().run_in_executor(
            None, _call_llm, system_prompt, task.description
        )
        analysis = _parse_analysis(raw, task)
    except Exception as exc:
        logger.error("LLM call failed: %s", exc)
        await agent.publish(
            "agent.error",
            {
                "error": str(exc),
                "step": "analysis",
                "recoverable": False,
            },
            workflow_id=event.workflow_id,
        )
        return None

    await agent.publish(
        "workflow.step",
        analysis.to_dict(),
        workflow_id=event.workflow_id,
        metadata={"step_role": "analyst"},
    )
    logger.info("Analyst published analysis complexity=%s", analysis.complexity)
    return b"ok"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(agent.run())
