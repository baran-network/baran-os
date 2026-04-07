"use client";

import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { EventFilterState } from "@/hooks/use-events";

// ---------------------------------------------------------------------------
// EventFilters — filter bar for the live event stream
// ---------------------------------------------------------------------------

const EVENT_TYPE_PREFIXES = [
  { label: "All types", value: "" },
  { label: "agent.*", value: "agent." },
  { label: "workflow.*", value: "workflow." },
  { label: "human.*", value: "human." },
  { label: "federation.*", value: "federation." },
  { label: "simulation.*", value: "simulation." },
  { label: "capability.*", value: "capability." },
  { label: "health.*", value: "health." },
  { label: "discovery.*", value: "discovery." },
];

interface EventFiltersProps {
  filters: EventFilterState;
  onChange: (next: Partial<EventFilterState>) => void;
}

export function EventFilters({ filters, onChange }: EventFiltersProps) {
  return (
    <div className="flex flex-wrap items-end gap-3 rounded-lg border border-border bg-card p-3">
      {/* Event type prefix */}
      <div className="flex min-w-[160px] flex-col gap-1">
        <label className="text-xs text-muted-foreground">Event type</label>
        <Select
          value={filters.typePrefix}
          onValueChange={(v) => onChange({ typePrefix: v ?? "" })}
        >
          <SelectTrigger className="h-8 text-sm">
            <SelectValue placeholder="All types" />
          </SelectTrigger>
          <SelectContent>
            {EVENT_TYPE_PREFIXES.map((opt) => (
              <SelectItem key={opt.value} value={opt.value}>
                {opt.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Agent filter */}
      <div className="flex min-w-[180px] flex-col gap-1">
        <label className="text-xs text-muted-foreground">Agent</label>
        <Input
          className="h-8 text-sm"
          placeholder="source or target agent"
          value={filters.agent}
          onChange={(e) => onChange({ agent: e.target.value })}
        />
      </div>

      {/* Workflow ID filter */}
      <div className="flex min-w-[220px] flex-col gap-1">
        <label className="text-xs text-muted-foreground">Workflow ID</label>
        <Input
          className="h-8 font-mono text-sm"
          placeholder="workflow UUID"
          value={filters.workflowId}
          onChange={(e) => onChange({ workflowId: e.target.value })}
        />
      </div>
    </div>
  );
}
