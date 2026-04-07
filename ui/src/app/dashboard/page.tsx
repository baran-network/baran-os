"use client";

import { useState } from "react";
import { useAgents } from "@/hooks/use-agents";
import { useStats } from "@/hooks/use-stats";
import { SummaryPanel } from "@/components/agents/summary-panel";
import { AgentFilters } from "@/components/agents/agent-filters";
import { AgentTable } from "@/components/agents/agent-table";
import { AgentDetailSheet } from "@/components/agents/agent-detail-sheet";
import type { Agent } from "@/lib/types";

export default function DashboardPage() {
  const { agents, filtered, loading, error, filters, setFilters } = useAgents();
  const { stats } = useStats(5000);
  const [selected, setSelected] = useState<Agent | null>(null);
  const [sheetOpen, setSheetOpen] = useState(false);

  const handleSelect = (agent: Agent) => {
    setSelected(agent);
    setSheetOpen(true);
  };

  return (
    <div className="space-y-4">
      <SummaryPanel stats={stats} />

      <AgentFilters agents={agents} filters={filters} onChange={setFilters} />

      {error && (
        <div className="rounded border border-red-300 bg-red-50 p-3 text-sm text-red-800">
          {error}
        </div>
      )}

      {loading ? (
        <div className="rounded-md border p-8 text-center text-sm text-muted-foreground">
          Loading agents…
        </div>
      ) : (
        <AgentTable
          agents={filtered}
          onSelect={handleSelect}
          selectedId={selected?.id}
        />
      )}

      <AgentDetailSheet
        agent={selected}
        open={sheetOpen}
        onOpenChange={setSheetOpen}
      />
    </div>
  );
}
