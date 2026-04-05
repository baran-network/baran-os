"""Domain payload dataclasses for the coding example workflow."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict


@dataclass
class CodingTask:
    """Input payload submitted by the trigger to start the workflow."""

    description: str
    language: str = "python"

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode()

    @classmethod
    def from_dict(cls, data: dict) -> "CodingTask":
        return cls(
            description=data["description"],
            language=data.get("language", "python"),
        )


@dataclass
class TaskAnalysis:
    """Output from the task analyst agent (code.analysis step)."""

    requirements: list[str] = field(default_factory=list)
    constraints: list[str] = field(default_factory=list)
    approach: str = ""
    language: str = "python"
    complexity: str = "medium"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "TaskAnalysis":
        return cls(
            requirements=data.get("requirements", []),
            constraints=data.get("constraints", []),
            approach=data.get("approach", ""),
            language=data.get("language", "python"),
            complexity=data.get("complexity", "medium"),
        )


@dataclass
class GeneratedCode:
    """Output from the code generator agent (code.generation step)."""

    code: str = ""
    explanation: str = ""
    language: str = "python"
    tools_used: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "GeneratedCode":
        return cls(
            code=data.get("code", ""),
            explanation=data.get("explanation", ""),
            language=data.get("language", "python"),
            tools_used=data.get("tools_used", []),
        )


@dataclass
class ReviewFeedback:
    """Output from the code reviewer agent (code.review step)."""

    verdict: str = "approve"
    issues: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)
    quality_score: int = 7

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ReviewFeedback":
        return cls(
            verdict=data.get("verdict", "approve"),
            issues=data.get("issues", []),
            suggestions=data.get("suggestions", []),
            quality_score=data.get("quality_score", 7),
        )


@dataclass
class WorkflowResult:
    """Aggregated result from all three workflow steps."""

    analysis: TaskAnalysis | None = None
    generated_code: GeneratedCode | None = None
    review: ReviewFeedback | None = None
    duration_seconds: float = 0.0
