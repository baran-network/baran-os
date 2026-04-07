"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { SSEManager } from "@/lib/sse";
import { buildSSEUrl, getAuthToken } from "@/lib/api";
import type { ConnectionState, Event } from "@/lib/types";

// ---------------------------------------------------------------------------
// useEvents — live event stream with ring buffer, pause/resume, RAF batching
// ---------------------------------------------------------------------------

const RING_BUFFER_CAP = 10_000;
const RAF_BATCH_SIZE = 100;
const HIGH_TRAFFIC_THRESHOLD = 50; // events/sec

export interface EventFilterState {
  typePrefix: string; // e.g., "agent." or "workflow."
  agent: string; // agent name/id substring
  workflowId: string;
}

const EMPTY_FILTERS: EventFilterState = {
  typePrefix: "",
  agent: "",
  workflowId: "",
};

export interface UseEventsReturn {
  events: Event[];
  paused: boolean;
  bufferedCount: number;
  isHighTraffic: boolean;
  connectionState: ConnectionState;
  filters: EventFilterState;
  setFilters: (next: Partial<EventFilterState>) => void;
  pause: () => void;
  resume: () => void;
  clear: () => void;
}

export function useEvents(): UseEventsReturn {
  const [events, setEvents] = useState<Event[]>([]);
  const [paused, setPaused] = useState(false);
  const [bufferedCount, setBufferedCount] = useState(0);
  const [isHighTraffic, setIsHighTraffic] = useState(false);
  const [connectionState, setConnectionState] =
    useState<ConnectionState>("disconnected");
  const [filters, setFiltersState] = useState<EventFilterState>(EMPTY_FILTERS);

  // Refs that don't trigger re-renders
  const pausedRef = useRef(false);
  const pendingRef = useRef<Event[]>([]); // buffer when paused
  const rafRef = useRef<number | null>(null);
  const managerRef = useRef<SSEManager | null>(null);
  const throughputRef = useRef<number[]>([]); // timestamps of recent events
  const filtersRef = useRef(filters);

  useEffect(() => {
    filtersRef.current = filters;
  }, [filters]);

  // -- Throughput tracking --------------------------------------------------

  const recordThroughput = useCallback(() => {
    const now = Date.now();
    throughputRef.current.push(now);
    // Keep only last 1s
    throughputRef.current = throughputRef.current.filter(
      (t) => now - t < 1_000,
    );
    setIsHighTraffic(throughputRef.current.length >= HIGH_TRAFFIC_THRESHOLD);
  }, []);

  // -- Filter helper --------------------------------------------------------

  const matchesFilters = useCallback((ev: Event): boolean => {
    const f = filtersRef.current;
    if (f.typePrefix && !ev.type.startsWith(f.typePrefix)) return false;
    if (
      f.agent &&
      !ev.source_agent.includes(f.agent) &&
      ev.target_agent?.includes(f.agent) !== true
    )
      return false;
    if (f.workflowId && ev.workflow_id !== f.workflowId) return false;
    return true;
  }, []);

  // -- RAF flush ------------------------------------------------------------

  const flush = useCallback(() => {
    rafRef.current = null;
    if (pendingRef.current.length === 0) return;

    const batch = pendingRef.current.splice(0, RAF_BATCH_SIZE);
    setBufferedCount(pendingRef.current.length);

    setEvents((prev) => {
      const next = [...prev, ...batch];
      return next.length > RING_BUFFER_CAP
        ? next.slice(next.length - RING_BUFFER_CAP)
        : next;
    });

    // If there's still pending, schedule another frame
    if (pendingRef.current.length > 0) {
      rafRef.current = requestAnimationFrame(flush);
    }
  }, []);

  const scheduleFlush = useCallback(() => {
    if (rafRef.current === null) {
      rafRef.current = requestAnimationFrame(flush);
    }
  }, [flush]);

  // -- SSE event handler ----------------------------------------------------

  const handleEvent = useCallback(
    (ev: Event) => {
      recordThroughput();
      if (!matchesFilters(ev)) return;

      if (pausedRef.current) {
        pendingRef.current.push(ev);
        setBufferedCount(pendingRef.current.length);
        return;
      }

      pendingRef.current.push(ev);
      scheduleFlush();
    },
    [recordThroughput, matchesFilters, scheduleFlush],
  );

  // -- SSE connection -------------------------------------------------------

  useEffect(() => {
    const manager = new SSEManager({
      url: buildSSEUrl("/api/events/stream"),
      token: getAuthToken(),
      onEvent: handleEvent,
      onStateChange: setConnectionState,
    });
    managerRef.current = manager;
    manager.connect();

    return () => {
      manager.disconnect();
      managerRef.current = null;
      if (rafRef.current !== null) {
        cancelAnimationFrame(rafRef.current);
        rafRef.current = null;
      }
    };
  }, [handleEvent]);

  // -- Controls -------------------------------------------------------------

  const pause = useCallback(() => {
    pausedRef.current = true;
    setPaused(true);
  }, []);

  const resume = useCallback(() => {
    pausedRef.current = false;
    setPaused(false);
    // Flush all buffered events
    scheduleFlush();
  }, [scheduleFlush]);

  const clear = useCallback(() => {
    pendingRef.current = [];
    setEvents([]);
    setBufferedCount(0);
  }, []);

  const setFilters = useCallback((next: Partial<EventFilterState>) => {
    setFiltersState((prev) => ({ ...prev, ...next }));
  }, []);

  return {
    events,
    paused,
    bufferedCount,
    isHighTraffic,
    connectionState,
    filters,
    setFilters,
    pause,
    resume,
    clear,
  };
}
