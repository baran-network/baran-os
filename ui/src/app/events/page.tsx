"use client";

import { useState } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { EventFilters } from "@/components/events/event-filters";
import { StreamControls } from "@/components/events/stream-controls";
import { EventStream } from "@/components/events/event-stream";
import { WorkflowSelector, WorkflowTimeline } from "@/components/events/workflow-timeline";
import { useEvents } from "@/hooks/use-events";
import { Badge } from "@/components/ui/badge";

// ---------------------------------------------------------------------------
// Events page — Live Stream + Workflow Timeline
// ---------------------------------------------------------------------------

export default function EventsPage() {
  const {
    events,
    paused,
    bufferedCount,
    isHighTraffic,
    filters,
    setFilters,
    pause,
    resume,
    clear,
  } = useEvents();

  const [selectedWorkflowId, setSelectedWorkflowId] = useState<string | null>(
    null,
  );

  return (
    <div className="flex h-full flex-col gap-4 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">Event Flow</h1>
          <p className="text-sm text-muted-foreground">
            Live event stream and workflow timelines
          </p>
        </div>

        {/* Event count badge */}
        <Badge variant="secondary" className="font-mono text-xs">
          {events.length.toLocaleString()} events
        </Badge>
      </div>

      <Tabs defaultValue="stream" className="flex flex-1 flex-col overflow-hidden">
        <TabsList className="shrink-0 w-fit">
          <TabsTrigger value="stream">Live Stream</TabsTrigger>
          <TabsTrigger value="timeline">Workflow Timeline</TabsTrigger>
        </TabsList>

        {/* ── Live Stream ── */}
        <TabsContent
          value="stream"
          className="flex flex-1 flex-col gap-3 overflow-hidden"
        >
          <div className="flex shrink-0 flex-wrap items-end gap-3">
            <EventFilters filters={filters} onChange={setFilters} />
            <StreamControls
              paused={paused}
              bufferedCount={bufferedCount}
              isHighTraffic={isHighTraffic}
              onPause={pause}
              onResume={resume}
              onClear={clear}
            />
          </div>

          <div className="flex-1 overflow-hidden rounded-lg border border-border bg-card">
            <EventStream events={events} className="h-full" />
          </div>
        </TabsContent>

        {/* ── Workflow Timeline ── */}
        <TabsContent
          value="timeline"
          className="flex flex-1 flex-col gap-4 overflow-auto"
        >
          <div className="flex items-center gap-3">
            <span className="text-sm text-muted-foreground">Workflow:</span>
            <WorkflowSelector
              selectedId={selectedWorkflowId}
              onSelect={setSelectedWorkflowId}
            />
          </div>

          {selectedWorkflowId ? (
            <WorkflowTimeline workflowId={selectedWorkflowId} />
          ) : (
            <div className="flex flex-1 items-center justify-center text-sm text-muted-foreground">
              Select a workflow to view its timeline
            </div>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
