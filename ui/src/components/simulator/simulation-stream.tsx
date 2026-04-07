"use client";

import { useMemo } from "react";
import { useEvents } from "@/hooks/use-events";
import { EventStream } from "@/components/events/event-stream";

// ---------------------------------------------------------------------------
// SimulationStream — reuses EventStream filtered to is_simulated=true
// ---------------------------------------------------------------------------

export function SimulationStream() {
  const { events } = useEvents();
  const simEvents = useMemo(
    () => events.filter((e) => e.is_simulated),
    [events],
  );

  return (
    <div className="flex h-full flex-col rounded-lg border bg-card">
      <div className="border-b px-3 py-2 text-xs font-semibold text-pink-600 dark:text-pink-400">
        SIMULATION STREAM ({simEvents.length})
      </div>
      <div className="flex-1 overflow-hidden">
        <EventStream events={simEvents} simulationMode className="h-full" />
      </div>
    </div>
  );
}
