"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { fetchDecisions, respondDecision } from "@/lib/api";
import { buildSSEUrl, getAuthToken } from "@/lib/api";
import type { Decision } from "@/lib/types";

export interface UseDecisionsReturn {
  pending: Decision[];
  history: Decision[];
  loading: boolean;
  error: string | null;
  respond: (
    decisionId: string,
    action: "approve" | "reject",
    comment?: string,
  ) => Promise<void>;
  refresh: () => Promise<void>;
}

export function useDecisions(): UseDecisionsReturn {
  const [pending, setPending] = useState<Decision[]>([]);
  const [history, setHistory] = useState<Decision[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const esRef = useRef<EventSource | null>(null);

  const refresh = useCallback(async () => {
    try {
      setError(null);
      const all = await fetchDecisions();
      setPending(all.filter((d) => !d.action));
      setHistory(all.filter((d) => !!d.action));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // SSE subscription to /api/decisions/stream for real-time updates
  useEffect(() => {
    const token = getAuthToken();
    const url = buildSSEUrl("/api/decisions/stream");
    const qs = token ? `${url.includes("?") ? "&" : "?"}token=${encodeURIComponent(token)}` : "";
    const es = new EventSource(url + qs);
    esRef.current = es;

    es.onmessage = () => {
      // Any decision event triggers a full refresh to keep state consistent
      void refresh();
    };

    es.addEventListener("decision.pending", () => {
      void refresh();
    });

    es.addEventListener("decision.resolved", () => {
      void refresh();
    });

    es.onerror = () => {
      // SSE reconnects automatically; errors are transient
    };

    return () => {
      es.close();
      esRef.current = null;
    };
  }, [refresh]);

  const respond = useCallback(
    async (
      decisionId: string,
      action: "approve" | "reject",
      comment?: string,
    ) => {
      await respondDecision(decisionId, action, "operator", comment);
      await refresh();
    },
    [refresh],
  );

  return { pending, history, loading, error, respond, refresh };
}
