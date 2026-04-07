"use client";

import { useEffect, useState } from "react";
import { fetchStats } from "@/lib/api";
import type { NetworkStats } from "@/lib/types";

export function useStats(intervalMs = 5000): {
  stats: NetworkStats | null;
  error: string | null;
} {
  const [stats, setStats] = useState<NetworkStats | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const s = await fetchStats();
        if (!cancelled) {
          setStats(s);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    };
    void tick();
    const id = setInterval(tick, intervalMs);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [intervalMs]);

  return { stats, error };
}
