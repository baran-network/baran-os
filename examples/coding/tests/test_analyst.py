"""Unit tests for the task analyst agent.

Tests verify that the analyst produces valid TaskAnalysis output
from natural-language coding task descriptions.

These tests require a real LLM API key and are skipped if not present.
"""

from __future__ import annotations

import json
import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import config
from models import CodingTask, TaskAnalysis


def _has_llm_key() -> bool:
    return bool(config.ANTHROPIC_API_KEY or config.OPENAI_API_KEY)


pytestmark = pytest.mark.skipif(not _has_llm_key(), reason="No LLM API key configured")


def test_analyst_prompt_loads():
    """Analyst prompt file exists and is non-empty."""
    prompt_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts", "analyst.txt")
    assert os.path.exists(prompt_path), "analyst.txt prompt file must exist"
    with open(prompt_path) as f:
        content = f.read()
    assert len(content) > 50, "analyst.txt should be a substantial prompt"


def test_task_analysis_from_dict():
    """TaskAnalysis correctly parses a valid dict."""
    data = {
        "requirements": ["check palindrome", "handle edge cases"],
        "constraints": ["case-insensitive", "handle empty string"],
        "approach": "Two-pointer comparison after normalization",
        "language": "python",
        "complexity": "low",
    }
    analysis = TaskAnalysis.from_dict(data)
    assert analysis.requirements == ["check palindrome", "handle edge cases"]
    assert analysis.language == "python"
    assert analysis.complexity == "low"


def test_task_analysis_to_dict_roundtrip():
    """TaskAnalysis serializes and deserializes correctly."""
    original = TaskAnalysis(
        requirements=["implement sorting"],
        constraints=["handle empty list"],
        approach="Use built-in sort",
        language="python",
        complexity="low",
    )
    restored = TaskAnalysis.from_dict(original.to_dict())
    assert restored.requirements == original.requirements
    assert restored.complexity == original.complexity


@pytest.mark.llm
def test_analyst_produces_valid_json_output():
    """Analyst LLM call returns parseable TaskAnalysis for a simple task."""
    from agents.analyst import _call_llm, _load_prompt, _parse_analysis

    task = CodingTask(description="Implement a function that reverses a string in Python")
    system_prompt = _load_prompt()
    raw = _call_llm(system_prompt, task.description)

    analysis = _parse_analysis(raw, task)
    assert isinstance(analysis.requirements, list)
    assert len(analysis.requirements) > 0
    assert analysis.language in ("python", "Python")
    assert analysis.complexity in ("low", "medium", "high")


@pytest.mark.llm
def test_analyst_handles_complex_task():
    """Analyst correctly identifies high complexity for a complex task."""
    from agents.analyst import _call_llm, _load_prompt, _parse_analysis

    task = CodingTask(
        description=(
            "Implement a concurrent task scheduler that supports priority queues, "
            "worker pools, retry logic with exponential backoff, and graceful shutdown."
        )
    )
    system_prompt = _load_prompt()
    raw = _call_llm(system_prompt, task.description)
    analysis = _parse_analysis(raw, task)

    assert analysis.complexity in ("medium", "high"), (
        f"Complex task should be medium or high complexity, got {analysis.complexity}"
    )
    assert len(analysis.requirements) >= 3
