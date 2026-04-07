"use client";

import { Badge } from "@/components/ui/badge";
import type { ReplaySession } from "@/lib/types";

// ---------------------------------------------------------------------------
// ReplayTimeline — visual scrubber showing replay position
// ---------------------------------------------------------------------------

interface ReplayTimelineProps {
  session: ReplaySession | null;
}

export function ReplayTimeline({ session }: ReplayTimelineProps) {
  if (!session) {
    return (
      <div className="rounded-lg border bg-card p-4 text-xs text-muted-foreground">
        No active replay session.
      </div>
    );
  }

  const total = session.total_events || 1;
  const replayed = session.replayed_events;
  const ratio = Math.min(1, replayed / total);

  return (
    <div className="flex flex-col gap-2 rounded-lg border bg-card p-4">
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span className="font-mono">
          workflow {session.workflow_id.slice(0, 12)}…
        </span>
        <Badge variant="outline">{session.state}</Badge>
      </div>
      <div className="relative h-6 w-full overflow-hidden rounded bg-muted">
        <div
          className="h-full bg-pink-500/70"
          style={{ width: `${ratio * 100}%` }}
        />
        <div
          className="absolute top-0 h-full w-0.5 bg-pink-700"
          style={{ left: `${ratio * 100}%` }}
        />
      </div>
      <div className="flex justify-between text-xs text-muted-foreground tabular-nums">
        <span>0</span>
        <span>
          {replayed} / {session.total_events}
        </span>
      </div>
    </div>
  );
}
