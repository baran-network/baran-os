"""Code generator agent — capability: code.generation.

Receives a TaskAnalysis event and generates source code using LangGraph
with MCP tool access for filesystem operations.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shlex
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from baran import BaranAgent, Capability
from baran.events import Event

import config
from models import TaskAnalysis, GeneratedCode

logger = logging.getLogger(__name__)

AGENT_NAME = "coding-generator"
CAPABILITY = "code.generation"


def _load_prompt() -> str:
    prompt_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts", "generator.txt")
    try:
        with open(prompt_path) as f:
            return f.read()
    except FileNotFoundError:
        return "You are a code generator. Generate complete, working source code based on the analysis."


def _call_llm_direct(system_prompt: str, user_message: str) -> str:
    if config.LLM_PROVIDER == "anthropic":
        import anthropic
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=2048,
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


async def _call_langgraph_with_mcp(system_prompt: str, analysis: TaskAnalysis) -> GeneratedCode:
    """Generate code using LangGraph with MCP filesystem tools.

    Falls back to direct LLM call if MCP server is unavailable.
    """
    tools_used: list[str] = []

    try:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
        from langchain_mcp_adapters.tools import load_mcp_tools
        from langgraph.prebuilt import create_react_agent

        if config.LLM_PROVIDER == "anthropic":
            from langchain_anthropic import ChatAnthropic
            llm = ChatAnthropic(model="claude-opus-4-6", api_key=config.ANTHROPIC_API_KEY)
        else:
            from langchain_openai import ChatOpenAI
            llm = ChatOpenAI(model="gpt-4o", api_key=config.OPENAI_API_KEY)

        cmd_parts = shlex.split(config.MCP_SERVER_CMD)
        server_params = StdioServerParameters(
            command=cmd_parts[0],
            args=cmd_parts[1:],
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await load_mcp_tools(session)

                agent = create_react_agent(llm, tools, prompt=system_prompt)
                user_msg = (
                    f"Generate code for:\n{json.dumps(analysis.to_dict(), indent=2)}"
                )
                result = await agent.ainvoke({"messages": [("user", user_msg)]})
                raw = result["messages"][-1].content

                # Extract tools actually called from ToolMessage entries
                for msg in result["messages"]:
                    if hasattr(msg, "type") and msg.type == "tool":
                        if msg.name and msg.name not in tools_used:
                            tools_used.append(msg.name)

    except Exception as exc:
        logger.warning(
            "MCP/LangGraph unavailable (%s: %s) — falling back to direct LLM call",
            type(exc).__name__,
            exc,
        )
        raw = await asyncio.get_running_loop().run_in_executor(
            None,
            _call_llm_direct,
            system_prompt,
            json.dumps(analysis.to_dict(), indent=2),
        )
        tools_used = []  # Ensure no stale tool data on fallback

    return _parse_code(raw, analysis, tools_used)


def _parse_code(raw: str, analysis: TaskAnalysis, tools_used: list[str]) -> GeneratedCode:
    try:
        data = json.loads(raw)
        result = GeneratedCode.from_dict(data)
        result.tools_used = tools_used or result.tools_used
        return result
    except (json.JSONDecodeError, KeyError):
        return GeneratedCode(
            code=raw,
            explanation="Generated code",
            language=analysis.language,
            tools_used=tools_used,
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
            description="Generate source code from structured analysis",
        )
    ],
    labels={"role": "generator", "example": "coding"},
)


@agent.on("workflow.step")
async def handle_workflow_step(event: Event) -> bytes | None:
    logger.info("Generator received workflow.step event_id=%s", event.event_id)
    try:
        analysis = TaskAnalysis.from_dict(event.payload)
    except (KeyError, TypeError) as exc:
        logger.error("Failed to parse TaskAnalysis: %s", exc)
        await agent.publish(
            "agent.error",
            {
                "error": f"Invalid payload: {exc}",
                "step": "generation",
                "recoverable": False,
            },
            workflow_id=event.workflow_id,
        )
        return None

    system_prompt = _load_prompt()
    try:
        generated = await _call_langgraph_with_mcp(system_prompt, analysis)
    except Exception as exc:
        logger.error("Code generation failed: %s", exc)
        await agent.publish(
            "agent.error",
            {
                "error": str(exc),
                "step": "generation",
                "recoverable": False,
            },
            workflow_id=event.workflow_id,
        )
        return None

    await agent.publish(
        "workflow.step",
        generated.to_dict(),
        workflow_id=event.workflow_id,
        metadata={"step_role": "generator"},
    )
    logger.info("Generator published code language=%s tools_used=%s", generated.language, generated.tools_used)
    return b"ok"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(agent.run())
