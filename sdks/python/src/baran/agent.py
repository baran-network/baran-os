"""BaranAgent — high-level API for building Baran OS agents in Python."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable, Coroutine
from typing import Any, Optional, Union

from .client import SidecarClient
from .events import Event
from .types import AgentConfig, Capability


EventHandler = Callable[[Event], Coroutine[Any, Any, Optional[bytes]]]


class BaranAgent:
    """A Baran OS agent that communicates via the sidecar gateway.

    Usage::

        agent = BaranAgent(name="my-agent", agent_type="worker", version="1.0.0", token="psk")

        @agent.on("workflow.step")
        async def handle(event: Event) -> bytes | None:
            return b"result"

        asyncio.run(agent.run())
    """

    def __init__(
        self,
        name: str,
        agent_type: str,
        version: str,
        token: str,
        *,
        sidecar_url: str = "http://localhost:9090",
        capabilities: list[Capability] | None = None,
        labels: dict[str, str] | None = None,
        agent_id: str | None = None,
    ) -> None:
        self._config = AgentConfig(
            name=name,
            agent_type=agent_type,
            version=version,
            sidecar_url=sidecar_url,
            token=token,
            capabilities=capabilities or [],
            labels=labels or {},
            agent_id=agent_id,
        )
        self._client = SidecarClient(sidecar_url, token)
        self._handlers: dict[str, EventHandler] = {}
        self._agent_id: str | None = agent_id
        self._running = False

    @property
    def agent_id(self) -> str | None:
        return self._agent_id

    def on(self, event_type: str) -> Callable[[EventHandler], EventHandler]:
        """Decorator to register an event handler for a given event type."""

        def decorator(fn: EventHandler) -> EventHandler:
            self._handlers[event_type] = fn
            return fn

        return decorator

    async def start(self) -> None:
        """Register the agent with the sidecar and prepare for events."""
        result = await self._client.register(self._config)
        self._agent_id = result["agent_id"]

    async def stop(self) -> None:
        """Deregister the agent and close connections."""
        self._running = False
        if self._agent_id:
            try:
                await self._client.deregister(self._agent_id)
            except Exception:
                pass
        await self._client.close()

    async def publish(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        target_agent: str | None = None,
        workflow_id: str | None = None,
        correlation_id: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Publish an event through the sidecar."""
        if not self._agent_id:
            raise RuntimeError("Agent not started — call start() first")
        return await self._client.publish(
            self._agent_id,
            event_type,
            payload,
            target_agent=target_agent,
            workflow_id=workflow_id,
            correlation_id=correlation_id,
            metadata=metadata,
        )

    async def run(self) -> None:
        """Start the agent, subscribe to events, and block until interrupted."""
        await self.start()
        self._running = True

        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)

        try:
            subscribe_task = asyncio.create_task(self._event_loop())
            stop_task = asyncio.create_task(stop_event.wait())
            done, pending = await asyncio.wait(
                {subscribe_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        finally:
            await self.stop()

    async def _event_loop(self) -> None:
        """Subscribe to SSE events and dispatch to registered handlers."""
        if not self._agent_id:
            return
        async for event in self._client.subscribe(self._agent_id):
            if not self._running:
                break
            handler = self._handlers.get(event.event_type)
            if handler is None:
                continue
            try:
                result = await handler(event)
                if result is not None:
                    await self._client.ack(self._agent_id, event.event_id)
            except Exception:
                pass

    async def __aenter__(self) -> BaranAgent:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()
