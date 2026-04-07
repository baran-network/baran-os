"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchAgents } from "@/lib/api";
import type { Agent, Event } from "@/lib/types";
import { useConnection } from "@/hooks/use-connection";

export interface AgentFilterState {
  q: string;
  status: string; // "" = all
  capability: string; // "" = all
  type: string; // "" = all
}

const EMPTY_FILTERS: AgentFilterState = {
  q: "",
  status: "",
  capability: "",
  type: "",
};

export interface UseAgentsReturn {
  agents: Agent[];
  filtered: Agent[];
  loading: boolean;
  error: string | null;
  filters: AgentFilterState;
  setFilters: (next: Partial<AgentFilterState>) => void;
  refresh: () => Promise<void>;
}

export function useAgents(): UseAgentsReturn {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFiltersState] = useState<AgentFilterState>(EMPTY_FILTERS);

  const refresh = useCallback(async () => {
    try {
      setError(null);
      const res = await fetchAgents();
      setAgents(res.agents);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // Refetch on any agent.* event from the global stream
  const handleEvent = useCallback(
    (ev: Event) => {
      if (ev.type.startsWith("agent.")) {
        void refresh();
      }
    },
    [refresh],
  );

  useConnection({ onEvent: handleEvent });

  const setFilters = useCallback((next: Partial<AgentFilterState>) => {
    setFiltersState((prev) => ({ ...prev, ...next }));
  }, []);

  const filtered = useMemo(() => {
    const q = filters.q.trim().toLowerCase();
    return agents.filter((a) => {
      if (q && !a.name.toLowerCase().includes(q)) return false;
      if (filters.status && a.status !== filters.status) return false;
      if (filters.type && a.type !== filters.type) return false;
      if (
        filters.capability &&
        !a.capabilities.some((c) => c.name === filters.capability)
      ) {
        return false;
      }
      return true;
    });
  }, [agents, filters]);

  return { agents, filtered, loading, error, filters, setFilters, refresh };
}
