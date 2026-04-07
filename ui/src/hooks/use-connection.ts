"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { SSEManager } from "@/lib/sse";
import { buildSSEUrl, getAuthToken } from "@/lib/api";
import type { ConnectionState, Event } from "@/lib/types";

export interface UseConnectionOptions {
  /** Called whenever a new event arrives from the global SSE stream. */
  onEvent?: (event: Event) => void;
  /** Called when a gap is detected in the stream. */
  onGap?: (skipped: number) => void;
  /** Additional SSE query params (e.g., type, agent, workflow_id filters). */
  params?: Record<string, string>;
  /** Whether the connection should be active. Default: true. */
  enabled?: boolean;
}

export interface UseConnectionReturn {
  state: ConnectionState;
  reconnect: () => void;
}

export function useConnection(
  opts: UseConnectionOptions = {},
): UseConnectionReturn {
  const { onEvent, onGap, params, enabled = true } = opts;
  const [state, setState] = useState<ConnectionState>("disconnected");
  const managerRef = useRef<SSEManager | null>(null);

  // Store latest callbacks in refs to avoid reconnecting on every render.
  const onEventRef = useRef(onEvent);
  const onGapRef = useRef(onGap);

  useEffect(() => {
    onEventRef.current = onEvent;
  }, [onEvent]);

  useEffect(() => {
    onGapRef.current = onGap;
  }, [onGap]);

  const connect = useCallback(() => {
    managerRef.current?.disconnect();

    const manager = new SSEManager({
      url: buildSSEUrl("/api/events/stream", params),
      token: getAuthToken(),
      onEvent: (ev) => onEventRef.current?.(ev),
      onGap: (gap) => onGapRef.current?.(gap.skipped),
      onStateChange: setState,
      onError: () => {
        // Error state is already handled via onStateChange → "disconnected"
      },
    });

    managerRef.current = manager;
    manager.connect();
  }, [params]);

  useEffect(() => {
    if (!enabled) {
      managerRef.current?.disconnect();
      return;
    }

    connect();

    return () => {
      managerRef.current?.disconnect();
      managerRef.current = null;
    };
  }, [enabled, connect]);

  const reconnect = useCallback(() => {
    connect();
  }, [connect]);

  return { state, reconnect };
}
