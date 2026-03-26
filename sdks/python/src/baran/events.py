"""Event dataclass representing a Baran OS event received by an agent."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Event:
    """An event received from the sidecar gateway."""

    event_id: str
    event_type: str
    source_agent: str
    source_node: str
    target_agent: str
    timestamp: int
    payload: dict
    workflow_id: str | None = None
    correlation_id: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)
