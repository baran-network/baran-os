"use client";

import { useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { fetchScenarioSession } from "@/lib/api";
import type { ScenarioSession } from "@/lib/types";

// ---------------------------------------------------------------------------
// ScenarioProgress — shows active scenario session status with polling
// ---------------------------------------------------------------------------

interface ScenarioProgressProps {
  session: ScenarioSession | null;
}

export function ScenarioProgress({ session: initial }: ScenarioProgressProps) {
  const [session, setSession] = useState<ScenarioSession | null>(initial);

  useEffect(() => {
    setSession(initial);
  }, [initial]);

  useEffect(() => {
    if (!session || session.state !== "running") return;
    const id = setInterval(async () => {
      try {
        const fresh = await fetchScenarioSession(session.id);
        setSession(fresh);
      } catch {
        // ignore
      }
    }, 1000);
    return () => clearInterval(id);
  }, [session?.id, session?.state]); // eslint-disable-line react-hooks/exhaustive-deps

  if (!session) {
    return (
      <div className="rounded-lg border bg-card p-4 text-xs text-muted-foreground">
        No scenario running.
      </div>
    );
  }

  const ratio = session.total_steps > 0 ? session.current_step / session.total_steps : 0;

  return (
    <div className="flex flex-col gap-2 rounded-lg border bg-card p-4">
      <div className="flex items-center justify-between text-sm">
        <span className="font-semibold">{session.scenario_name}</span>
        <Badge variant="outline">{session.state}</Badge>
      </div>
      <div className="text-xs text-muted-foreground tabular-nums">
        Step {session.current_step} / {session.total_steps} ·{" "}
        {session.injected_events} events injected
      </div>
      <div className="h-2 w-full overflow-hidden rounded bg-muted">
        <div
          className="h-full bg-pink-500 transition-all"
          style={{ width: `${ratio * 100}%` }}
        />
      </div>
      {session.error_message && (
        <div className="text-xs text-destructive">{session.error_message}</div>
      )}
    </div>
  );
}
