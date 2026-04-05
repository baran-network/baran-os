"""Unit tests for the code generator agent.

Tests verify that the generator produces valid GeneratedCode output
from a structured TaskAnalysis, with and without MCP tool access.
"""

from __future__ import annotations

import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import config
from models import TaskAnalysis, GeneratedCode


def _has_llm_key() -> bool:
    return bool(config.ANTHROPIC_API_KEY or config.OPENAI_API_KEY)


pytestmark = pytest.mark.skipif(not _has_llm_key(), reason="No LLM API key configured")


def test_generator_prompt_loads():
    """Generator prompt file exists and contains MCP tool-use instructions."""
    prompt_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts", "generator.txt")
    assert os.path.exists(prompt_path), "generator.txt prompt file must exist"
    with open(prompt_path) as f:
        content = f.read()
    assert len(content) > 50
    assert "tool" in content.lower(), "generator.txt should mention tools"


def test_generated_code_from_dict():
    """GeneratedCode correctly parses a valid dict."""
    data = {
        "code": "def reverse(s): return s[::-1]",
        "explanation": "Uses Python slice notation",
        "language": "python",
        "tools_used": [],
    }
    generated = GeneratedCode.from_dict(data)
    assert "reverse" in generated.code
    assert generated.language == "python"
    assert generated.tools_used == []


def test_generated_code_roundtrip():
    """GeneratedCode serializes and deserializes correctly."""
    original = GeneratedCode(
        code="def foo(): pass",
        explanation="A placeholder",
        language="python",
        tools_used=["filesystem.read_file"],
    )
    restored = GeneratedCode.from_dict(original.to_dict())
    assert restored.code == original.code
    assert restored.tools_used == original.tools_used


@pytest.mark.llm
def test_generator_produces_valid_code():
    """Generator LLM call returns parseable GeneratedCode for a simple analysis."""
    import asyncio
    from agents.generator import _call_llm_direct, _load_prompt, _parse_code

    analysis = TaskAnalysis(
        requirements=["reverse a string"],
        constraints=["handle empty string", "preserve Unicode"],
        approach="Use slice notation",
        language="python",
        complexity="low",
    )
    system_prompt = _load_prompt()
    import json

    raw = _call_llm_direct(system_prompt, json.dumps(analysis.to_dict(), indent=2))
    generated = _parse_code(raw, analysis, [])

    assert len(generated.code) > 10, "Generated code should be non-trivial"
    assert generated.language == "python"


@pytest.mark.llm
@pytest.mark.mcp
def test_generator_with_mcp_tools():
    """Generator uses MCP tools and populates tools_used field.

    When the MCP server is available, the generator should invoke at least
    one filesystem tool and record it in tools_used.  If MCP is unavailable,
    the graceful-degradation path still returns valid GeneratedCode.
    """
    import asyncio
    from agents.generator import _call_langgraph_with_mcp, _load_prompt

    analysis = TaskAnalysis(
        requirements=["read a file and return its line count"],
        constraints=["handle non-existent file gracefully"],
        approach="Use MCP filesystem tool to read the file, then count lines",
        language="python",
        complexity="low",
    )
    system_prompt = _load_prompt()

    async def run():
        return await _call_langgraph_with_mcp(system_prompt, analysis)

    generated = asyncio.run(run())

    # Core validity — always holds regardless of MCP availability
    assert isinstance(generated, GeneratedCode)
    assert len(generated.code) > 10, "Generated code should be non-trivial"
    assert generated.language == "python"
    assert isinstance(generated.tools_used, list)

    # When MCP server is reachable, tools_used should be populated
    if generated.tools_used:
        assert all(isinstance(t, str) for t in generated.tools_used)
        assert any("file" in t.lower() or "read" in t.lower() or "list" in t.lower()
                    for t in generated.tools_used), (
            f"Expected a filesystem-related tool, got: {generated.tools_used}"
        )


@pytest.mark.llm
@pytest.mark.mcp
def test_generator_mcp_degradation():
    """Generator falls back to direct LLM when MCP server is unavailable."""
    import asyncio
    from unittest.mock import patch
    from agents.generator import _call_langgraph_with_mcp, _load_prompt

    analysis = TaskAnalysis(
        requirements=["reverse a string"],
        constraints=["handle empty string"],
        approach="Use slice notation",
        language="python",
        complexity="low",
    )
    system_prompt = _load_prompt()

    # Force MCP to fail by pointing to a non-existent command
    async def run():
        with patch("agents.generator.config") as mock_cfg:
            mock_cfg.LLM_PROVIDER = config.LLM_PROVIDER
            mock_cfg.ANTHROPIC_API_KEY = config.ANTHROPIC_API_KEY
            mock_cfg.OPENAI_API_KEY = config.OPENAI_API_KEY
            mock_cfg.MCP_SERVER_CMD = "nonexistent-mcp-server-command"
            return await _call_langgraph_with_mcp(system_prompt, analysis)

    generated = asyncio.run(run())

    # Should still produce valid code via fallback
    assert isinstance(generated, GeneratedCode)
    assert len(generated.code) > 10
    # Fallback path should have no tools_used
    assert generated.tools_used == []
