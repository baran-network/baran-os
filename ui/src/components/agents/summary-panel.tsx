"use client";

import { Card, CardContent, CardDescription, CardTitle } from "@/components/ui/card";
import type { NetworkStats } from "@/lib/types";

interface Props {
  stats: NetworkStats | null;
}

function StatCard({
  label,
  value,
  tone,
}: {
  label: string;
  value: number | string;
  tone?: "ok" | "warn" | "bad" | "muted";
}) {
  const color =
    tone === "ok"
      ? "text-green-600"
      : tone === "warn"
        ? "text-yellow-600"
        : tone === "bad"
          ? "text-red-600"
          : "text-foreground";
  return (
    <Card>
      <CardContent className="p-4">
        <CardDescription className="text-xs uppercase tracking-wide">
          {label}
        </CardDescription>
        <CardTitle className={`mt-1 text-2xl font-semibold ${color}`}>
          {value}
        </CardTitle>
      </CardContent>
    </Card>
  );
}

export function SummaryPanel({ stats }: Props) {
  const dash = (v: number | undefined) => (v ?? 0).toLocaleString();
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-8">
      <StatCard label="Agents" value={dash(stats?.total_agents)} />
      <StatCard label="Healthy" value={dash(stats?.healthy_count)} tone="ok" />
      <StatCard label="Degraded" value={dash(stats?.degraded_count)} tone="warn" />
      <StatCard label="Offline" value={dash(stats?.offline_count)} tone="bad" />
      <StatCard
        label="Throughput/s"
        value={dash(stats?.event_throughput)}
      />
      <StatCard label="Workflows" value={dash(stats?.active_workflows)} />
      <StatCard
        label="Decisions"
        value={dash(stats?.pending_decisions)}
        tone={stats && stats.pending_decisions > 0 ? "warn" : "muted"}
      />
      <StatCard label="Federation" value={dash(stats?.federation_nodes)} />
    </div>
  );
}
