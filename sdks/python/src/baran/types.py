"""Shared types used throughout the Baran SDK."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Capability:
    """Declares a capability this agent can handle."""

    name: str
    version: str
    description: str = ""
    parameters: dict[str, str] | None = None


@dataclass
class AgentConfig:
    """Full configuration for a BaranAgent."""

    name: str
    agent_type: str
    version: str
    sidecar_url: str = "http://localhost:9090"
    token: str = ""
    capabilities: list[Capability] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)
    agent_id: str | None = None
