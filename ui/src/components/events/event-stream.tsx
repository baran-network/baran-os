"use client";

import { useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronRight } from "lucide-react";
import { cn, dotCategory, formatTimestampTime } from "@/lib/utils";
import type { Event } from "@/lib/types";

// ---------------------------------------------------------------------------
// EventStream — virtualized, scrollable list of events with payload preview
// ---------------------------------------------------------------------------

// Color mapping per event category
const CATEGORY_COLORS: Record<string, string> = {
  agent: "bg-blue-500",
  workflow: "bg-violet-500",
  human: "bg-orange-500",
  federation: "bg-teal-500",
  simulation: "bg-pink-500",
  capability: "bg-emerald-500",
  health: "bg-green-500",
  discovery: "bg-sky-500",
};

function categoryColor(eventType: string): string {
  const cat = dotCategory(eventType);
  return CATEGORY_COLORS[cat] ?? "bg-gray-500";
}

// ---------------------------------------------------------------------------
// Row
// ---------------------------------------------------------------------------

interface EventRowProps {
  event: Event;
  /** When true, uses a distinct sim color scheme */
  simulationMode?: boolean;
}

function EventRow({ event, simulationMode }: EventRowProps) {
  const [expanded, setExpanded] = useState(false);
  const hasPayload =
    event.payload && Object.keys(event.payload).length > 0;

  return (
    <div
      className={cn(
        "border-b border-border px-3 py-1.5 text-xs font-mono hover:bg-accent/40 cursor-pointer",
        simulationMode && "border-l-2 border-l-pink-400",
      )}
      onClick={() => hasPayload && setExpanded((p) => !p)}
    >
      {/* Main row */}
      <div className="flex items-center gap-2 min-w-0">
        {/* Expand chevron */}
        <span className="shrink-0 text-muted-foreground w-3">
          {hasPayload ? (
            expanded ? (
              <ChevronDown className="h-3 w-3" />
            ) : (
              <ChevronRight className="h-3 w-3" />
            )
          ) : null}
        </span>

        {/* Timestamp */}
        <span className="shrink-0 text-muted-foreground w-20 tabular-nums">
          {formatTimestampTime(event.timestamp)}
        </span>

        {/* Category dot */}
        <span
          className={cn(
            "shrink-0 h-2 w-2 rounded-full",
            categoryColor(event.type),
          )}
        />

        {/* Event type */}
        <span className="shrink-0 font-semibold text-foreground min-w-[160px]">
          {event.type}
        </span>

        {/* Source → Target */}
        <span className="truncate text-muted-foreground">
          <span className="text-foreground">{event.source_agent}</span>
          {event.target_agent && (
            <>
              <span className="mx-1">→</span>
              <span className="text-foreground">{event.target_agent}</span>
            </>
          )}
        </span>

        {/* Simulation badge */}
        {event.is_simulated && (
          <Badge
            variant="outline"
            className="ml-auto shrink-0 border-pink-400 text-pink-600 dark:text-pink-400 text-[10px] px-1 py-0"
          >
            SIM
          </Badge>
        )}

        {/* Workflow ID (truncated) */}
        {event.workflow_id && (
          <span className="shrink-0 text-[10px] text-muted-foreground font-mono">
            {event.workflow_id.slice(0, 8)}…
          </span>
        )}
      </div>

      {/* Expanded payload preview */}
      {expanded && hasPayload && (
        <div className="mt-1.5 ml-5 rounded bg-muted/60 p-2 text-[11px] leading-relaxed text-muted-foreground overflow-x-auto">
          <pre className="whitespace-pre-wrap break-all">
            {JSON.stringify(event.payload, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// EventStream
// ---------------------------------------------------------------------------

interface EventStreamProps {
  events: Event[];
  /** When true, applies simulation color scheme (pink border, labels) */
  simulationMode?: boolean;
  /** Extra class for the container */
  className?: string;
}

export function EventStream({
  events,
  simulationMode = false,
  className,
}: EventStreamProps) {
  const parentRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);

  const virtualizer = useVirtualizer({
    count: events.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 32,
    overscan: 20,
  });

  // Auto-scroll to bottom when new events arrive
  const items = virtualizer.getVirtualItems();

  // Detect manual scroll up → disable auto-scroll
  const handleScroll = () => {
    const el = parentRef.current;
    if (!el) return;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 60;
    setAutoScroll(atBottom);
  };

  // Scroll to bottom button
  const scrollToBottom = () => {
    virtualizer.scrollToIndex(events.length - 1, { align: "end" });
    setAutoScroll(true);
  };

  if (events.length === 0) {
    return (
      <div
        className={cn(
          "flex h-full items-center justify-center text-sm text-muted-foreground",
          className,
        )}
      >
        Waiting for events…
      </div>
    );
  }

  return (
    <div className={cn("relative flex flex-col", className)}>
      <div
        ref={parentRef}
        className="overflow-y-auto flex-1 h-full"
        onScroll={handleScroll}
      >
        <div
          style={{
            height: `${virtualizer.getTotalSize()}px`,
            width: "100%",
            position: "relative",
          }}
        >
          {items.map((item) => (
            <div
              key={item.key}
              style={{
                position: "absolute",
                top: 0,
                left: 0,
                width: "100%",
                transform: `translateY(${item.start}px)`,
              }}
              ref={virtualizer.measureElement}
              data-index={item.index}
            >
              <EventRow
                event={events[item.index]}
                simulationMode={simulationMode}
              />
            </div>
          ))}
        </div>
      </div>

      {/* Scroll to bottom button */}
      {!autoScroll && (
        <div className="absolute bottom-4 right-4">
          <Button
            size="sm"
            variant="secondary"
            onClick={scrollToBottom}
            className="shadow-md"
          >
            <ChevronDown className="h-4 w-4" />
            Latest
          </Button>
        </div>
      )}
    </div>
  );
}
