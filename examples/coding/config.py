"""Shared configuration for the coding example agents."""

from __future__ import annotations

import os


LLM_PROVIDER: str = os.environ.get("LLM_PROVIDER", "anthropic")

ANTHROPIC_API_KEY: str | None = os.environ.get("ANTHROPIC_API_KEY")
OPENAI_API_KEY: str | None = os.environ.get("OPENAI_API_KEY")

SIDECAR_URL: str = os.environ.get("SIDECAR_URL", "http://localhost:9090")
BARAN_PSK: str = os.environ.get("BARAN_PSK", "")

MCP_SERVER_CMD: str = os.environ.get(
    "MCP_SERVER_CMD",
    "npx @modelcontextprotocol/server-filesystem .",
)

AGENT_VERSION: str = "1.0.0"
