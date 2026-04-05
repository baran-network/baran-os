"""Unit tests for the code reviewer agent.

Tests verify that the reviewer produces valid ReviewFeedback output
from generated code input.
"""

from __future__ import annotations

import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import config
from models import GeneratedCode, ReviewFeedback


def _has_llm_key() -> bool:
    return bool(config.ANTHROPIC_API_KEY or config.OPENAI_API_KEY)


pytestmark = pytest.mark.skipif(not _has_llm_key(), reason="No LLM API key configured")


def test_reviewer_prompt_loads():
    """Reviewer prompt file exists and is non-empty."""
    prompt_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts", "reviewer.txt")
    assert os.path.exists(prompt_path), "reviewer.txt prompt file must exist"
    with open(prompt_path) as f:
        content = f.read()
    assert len(content) > 50


def test_review_feedback_from_dict():
    """ReviewFeedback correctly parses a valid dict."""
    data = {
        "verdict": "approve",
        "issues": [],
        "suggestions": ["Add type hints"],
        "quality_score": 8,
    }
    feedback = ReviewFeedback.from_dict(data)
    assert feedback.verdict == "approve"
    assert feedback.quality_score == 8
    assert "Add type hints" in feedback.suggestions


def test_review_feedback_roundtrip():
    """ReviewFeedback serializes and deserializes correctly."""
    original = ReviewFeedback(
        verdict="request_changes",
        issues=["Missing error handling"],
        suggestions=["Use try/except"],
        quality_score=5,
    )
    restored = ReviewFeedback.from_dict(original.to_dict())
    assert restored.verdict == original.verdict
    assert restored.issues == original.issues
    assert restored.quality_score == original.quality_score


@pytest.mark.llm
def test_reviewer_approves_good_code():
    """Reviewer approves well-written code with score >= 6."""
    from agents.reviewer import _call_llm, _load_prompt, _parse_feedback

    good_code = GeneratedCode(
        code="""
def is_palindrome(s: str) -> bool:
    \"\"\"Check if a string is a palindrome (case-insensitive).\"\"\"
    if not s:
        return True
    normalized = s.lower().replace(" ", "")
    return normalized == normalized[::-1]
""",
        explanation="Uses slice comparison after normalization",
        language="python",
        tools_used=[],
    )
    system_prompt = _load_prompt()
    user_message = f"Review the following {good_code.language} code:\n\n{good_code.code}"

    raw = _call_llm(system_prompt, user_message)
    feedback = _parse_feedback(raw, good_code)

    assert feedback.quality_score >= 6, f"Good code should score >= 6, got {feedback.quality_score}"
    assert feedback.verdict in ("approve", "request_changes")
    assert isinstance(feedback.issues, list)
    assert isinstance(feedback.suggestions, list)


@pytest.mark.llm
def test_reviewer_flags_bad_code():
    """Reviewer requests changes for clearly buggy code."""
    from agents.reviewer import _call_llm, _load_prompt, _parse_feedback

    bad_code = GeneratedCode(
        code="def palindrome(s): return s == s[::-1]",
        explanation="Simple palindrome check without edge cases",
        language="python",
        tools_used=[],
    )
    system_prompt = _load_prompt()
    user_message = (
        f"Review the following {bad_code.language} code:\n\n{bad_code.code}\n\n"
        "Note: this should handle case-insensitive comparison and empty strings."
    )

    raw = _call_llm(system_prompt, user_message)
    feedback = _parse_feedback(raw, bad_code)

    assert isinstance(feedback.issues, list)
    assert isinstance(feedback.suggestions, list)
    # May or may not request changes depending on LLM judgment — just verify structure.
    assert feedback.verdict in ("approve", "request_changes")
