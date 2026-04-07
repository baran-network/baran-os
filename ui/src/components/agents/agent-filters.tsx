"use client";

import { useMemo } from "react";
import { Input } from "@/components/ui/input";
import type { Agent } from "@/lib/types";
import type { AgentFilterState } from "@/hooks/use-agents";

interface Props {
  agents: Agent[];
  filters: AgentFilterState;
  onChange: (next: Partial<AgentFilterState>) => void;
}

const STATUS_OPTIONS = [
  { value: "", label: "All statuses" },
  { value: "active", label: "Active" },
  { value: "unhealthy", label: "Unhealthy" },
  { value: "dead", label: "Dead" },
  { value: "unregistered", label: "Unregistered" },
  { value: "unknown", label: "Unknown" },
];

export function AgentFilters({ agents, filters, onChange }: Props) {
  const types = useMemo(
    () => Array.from(new Set(agents.map((a) => a.type))).sort(),
    [agents],
  );
  const capabilities = useMemo(
    () =>
      Array.from(
        new Set(agents.flatMap((a) => a.capabilities.map((c) => c.name))),
      ).sort(),
    [agents],
  );

  return (
    <div className="flex flex-wrap items-center gap-2">
      <Input
        placeholder="Search by name…"
        value={filters.q}
        onChange={(e) => onChange({ q: e.target.value })}
        className="max-w-xs"
      />
      <select
        value={filters.status}
        onChange={(e) => onChange({ status: e.target.value })}
        className="h-9 rounded-md border bg-background px-2 text-sm"
      >
        {STATUS_OPTIONS.map((s) => (
          <option key={s.value} value={s.value}>
            {s.label}
          </option>
        ))}
      </select>
      <select
        value={filters.capability}
        onChange={(e) => onChange({ capability: e.target.value })}
        className="h-9 rounded-md border bg-background px-2 text-sm"
      >
        <option value="">All capabilities</option>
        {capabilities.map((c) => (
          <option key={c} value={c}>
            {c}
          </option>
        ))}
      </select>
      <select
        value={filters.type}
        onChange={(e) => onChange({ type: e.target.value })}
        className="h-9 rounded-md border bg-background px-2 text-sm"
      >
        <option value="">All types</option>
        {types.map((t) => (
          <option key={t} value={t}>
            {t}
          </option>
        ))}
      </select>
    </div>
  );
}
