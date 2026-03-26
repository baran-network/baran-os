"""Integration tests for the Baran Python SDK.

These tests require a live sidecar gateway. Start one with:
    baran-sidecar --nats-url nats://localhost:4222 --psk test-secret

Set BARAN_SIDECAR_URL to override the default (http://localhost:9090).
"""

from __future__ import annotations

import asyncio
import os

import pytest

from baran import BaranAgent, Capability, Event
from baran.client import SidecarClient
from baran.exceptions import BaranAuthError, BaranConnectionError, BaranRegistrationError

SIDECAR_URL = os.environ.get("BARAN_SIDECAR_URL", "http://localhost:9090")
PSK = os.environ.get("BARAN_SIDECAR_PSK", "test-secret")


def _skip_if_no_sidecar() -> None:
    """Skip test if sidecar is not reachable."""
    import httpx

    try:
        resp = httpx.get(f"{SIDECAR_URL}/health", timeout=2.0)
        if resp.status_code not in (200, 503):
            pytest.skip("Sidecar not reachable")
    except Exception:
        pytest.skip("Sidecar not reachable")


@pytest.fixture(autouse=True)
def require_sidecar():
    _skip_if_no_sidecar()


class TestSidecarClient:
    """Test the low-level HTTP client against a live sidecar."""

    async def test_register_and_deregister(self):
        client = SidecarClient(SIDECAR_URL, PSK)
        try:
            from baran.types import AgentConfig

            config = AgentConfig(
                name="test-client-agent",
                agent_type="tester",
                version="1.0.0",
                sidecar_url=SIDECAR_URL,
                token=PSK,
                capabilities=[Capability(name="test.echo", version="1.0.0")],
            )
            result = await client.register(config)
            assert "agent_id" in result
            assert result["status"] == "active"
            agent_id = result["agent_id"]

            dereg = await client.deregister(agent_id)
            assert dereg["status"] == "deregistered"
        finally:
            await client.close()

    async def test_register_with_bad_auth(self):
        client = SidecarClient(SIDECAR_URL, "wrong-token")
        try:
            from baran.types import AgentConfig

            config = AgentConfig(
                name="bad-auth-agent",
                agent_type="tester",
                version="1.0.0",
            )
            with pytest.raises(BaranAuthError):
                await client.register(config)
        finally:
            await client.close()

    async def test_publish_event(self):
        client = SidecarClient(SIDECAR_URL, PSK)
        try:
            from baran.types import AgentConfig

            config = AgentConfig(
                name="publisher-agent",
                agent_type="tester",
                version="1.0.0",
                capabilities=[Capability(name="test.publish", version="1.0.0")],
            )
            reg = await client.register(config)
            agent_id = reg["agent_id"]

            result = await client.publish(
                agent_id,
                "agent.health.ping",
                {},
            )
            assert "event_id" in result

            await client.deregister(agent_id)
        finally:
            await client.close()


class TestBaranAgent:
    """Test the high-level BaranAgent class against a live sidecar."""

    async def test_start_stop_lifecycle(self):
        agent = BaranAgent(
            name="lifecycle-agent",
            agent_type="tester",
            version="1.0.0",
            token=PSK,
            sidecar_url=SIDECAR_URL,
            capabilities=[Capability(name="test.lifecycle", version="1.0.0")],
        )
        await agent.start()
        assert agent.agent_id is not None
        await agent.stop()

    async def test_context_manager(self):
        async with BaranAgent(
            name="ctx-agent",
            agent_type="tester",
            version="1.0.0",
            token=PSK,
            sidecar_url=SIDECAR_URL,
            capabilities=[Capability(name="test.ctx", version="1.0.0")],
        ) as agent:
            assert agent.agent_id is not None

    async def test_publish_via_agent(self):
        async with BaranAgent(
            name="pub-agent",
            agent_type="tester",
            version="1.0.0",
            token=PSK,
            sidecar_url=SIDECAR_URL,
            capabilities=[Capability(name="test.pub", version="1.0.0")],
        ) as agent:
            result = await agent.publish(
                "agent.health.ping",
                {},
            )
            assert "event_id" in result

    async def test_event_handler_registration(self):
        agent = BaranAgent(
            name="handler-agent",
            agent_type="tester",
            version="1.0.0",
            token=PSK,
            sidecar_url=SIDECAR_URL,
        )

        received: list[Event] = []

        @agent.on("workflow.step")
        async def handle(event: Event) -> bytes | None:
            received.append(event)
            return b"ok"

        assert "workflow.step" in agent._handlers
