"""HTTP/SSE client for communicating with the Baran OS Sidecar Gateway."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
import httpx_sse

from .events import Event
from .exceptions import (
    BaranAuthError,
    BaranConnectionError,
    BaranPublishError,
    BaranRegistrationError,
)
from .types import AgentConfig, Capability


def _build_register_body(config: AgentConfig) -> dict[str, Any]:
    body: dict[str, Any] = {
        "name": config.name,
        "agent_type": config.agent_type,
        "version": config.version,
    }
    if config.agent_id:
        body["agent_id"] = config.agent_id
    if config.capabilities:
        body["capabilities"] = [
            {
                "name": c.name,
                "version": c.version,
                "description": c.description,
                **({"parameters": c.parameters} if c.parameters else {}),
            }
            for c in config.capabilities
        ]
    if config.labels:
        body["labels"] = config.labels
    return body


def _raise_for_status(resp: httpx.Response) -> None:
    if resp.status_code == 401:
        raise BaranAuthError(f"Authentication failed: {resp.text}")
    if resp.status_code >= 400:
        try:
            body = resp.json()
            msg = body.get("error", resp.text)
        except Exception:
            msg = resp.text
        if resp.status_code == 409:
            raise BaranRegistrationError(f"Agent conflict: {msg}")
        if resp.status_code == 503:
            raise BaranRegistrationError(f"Service unavailable: {msg}")
        raise BaranConnectionError(f"HTTP {resp.status_code}: {msg}")


class SidecarClient:
    """Low-level async HTTP client for the sidecar gateway."""

    def __init__(self, base_url: str, token: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._http: httpx.AsyncClient | None = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                base_url=self._base_url,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json",
                },
                timeout=httpx.Timeout(30.0, connect=10.0),
            )
        return self._http

    async def close(self) -> None:
        if self._http and not self._http.is_closed:
            await self._http.aclose()
            self._http = None

    async def register(self, config: AgentConfig) -> dict[str, Any]:
        client = await self._client()
        body = _build_register_body(config)
        try:
            resp = await client.post("/agents", json=body)
        except httpx.ConnectError as exc:
            raise BaranConnectionError(f"Cannot reach sidecar at {self._base_url}: {exc}") from exc
        _raise_for_status(resp)
        return resp.json()

    async def deregister(self, agent_id: str) -> dict[str, Any]:
        client = await self._client()
        try:
            resp = await client.delete(f"/agents/{agent_id}")
        except httpx.ConnectError as exc:
            raise BaranConnectionError(f"Cannot reach sidecar: {exc}") from exc
        _raise_for_status(resp)
        return resp.json()

    async def publish(
        self,
        agent_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        target_agent: str | None = None,
        workflow_id: str | None = None,
        correlation_id: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        client = await self._client()
        body: dict[str, Any] = {"type": event_type, "payload": payload}
        if target_agent:
            body["target_agent"] = target_agent
        if workflow_id:
            body["workflow_id"] = workflow_id
        if correlation_id:
            body["correlation_id"] = correlation_id
        if metadata:
            body["metadata"] = metadata
        try:
            resp = await client.post(f"/agents/{agent_id}/events", json=body)
        except httpx.ConnectError as exc:
            raise BaranConnectionError(f"Cannot reach sidecar: {exc}") from exc
        if resp.status_code >= 400:
            raise BaranPublishError(f"Publish failed ({resp.status_code}): {resp.text}")
        return resp.json()

    async def ack(self, agent_id: str, event_id: str) -> None:
        client = await self._client()
        try:
            resp = await client.post(
                f"/agents/{agent_id}/ack",
                json={"event_id": event_id},
            )
        except httpx.ConnectError as exc:
            raise BaranConnectionError(f"Cannot reach sidecar: {exc}") from exc
        _raise_for_status(resp)

    async def subscribe(
        self,
        agent_id: str,
        last_event_id: str | None = None,
    ) -> AsyncIterator[Event]:
        client = await self._client()
        headers: dict[str, str] = {"Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id

        try:
            async with httpx_sse.aconnect_sse(
                client,
                "GET",
                f"/agents/{agent_id}/events",
                headers=headers,
            ) as sse:
                async for event in sse.aiter_sse():
                    if not event.event or event.event == "keepalive":
                        continue
                    if event.event == "error":
                        continue
                    yield _parse_sse_event(event)
        except httpx.ConnectError as exc:
            raise BaranConnectionError(f"Cannot reach sidecar: {exc}") from exc


def _parse_sse_event(sse: httpx_sse.ServerSentEvent) -> Event:
    import json

    data = json.loads(sse.data)
    return Event(
        event_id=sse.id or "",
        event_type=sse.event or "",
        source_agent=data.get("source_agent", ""),
        source_node=data.get("source_node", ""),
        target_agent=data.get("target_agent", ""),
        timestamp=data.get("timestamp", 0),
        payload=data.get("payload", {}),
        workflow_id=data.get("workflow_id"),
        correlation_id=data.get("correlation_id"),
        metadata=data.get("metadata", {}),
    )
