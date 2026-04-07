"use client";

import { useEffect, useMemo, useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { AgentStatusBadge } from "./agent-status-badge";
import { useConnection } from "@/hooks/use-connection";
import { formatBytes, formatRelativeTime } from "@/lib/utils";
import type { Agent, Capability, Event } from "@/lib/types";

interface Props {
  agent: Agent | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const MAX_LIVE_EVENTS = 100;

export function AgentDetailSheet({ agent, open, onOpenChange }: Props) {
  const [liveEvents, setLiveEvents] = useState<Event[]>([]);

  // Reset events whenever the selected agent changes
  useEffect(() => {
    setLiveEvents([]);
  }, [agent?.id]);

  useConnection({
    enabled: open && !!agent,
    onEvent: (ev) => {
      if (!agent) return;
      if (ev.source_agent === agent.id || ev.target_agent === agent.id) {
        setLiveEvents((prev) => {
          const next = [ev, ...prev];
          return next.length > MAX_LIVE_EVENTS
            ? next.slice(0, MAX_LIVE_EVENTS)
            : next;
        });
      }
    },
  });

  const capsByCategory = useMemo(() => {
    if (!agent) return {} as Record<string, Capability[]>;
    return agent.capabilities.reduce<Record<string, Capability[]>>(
      (acc, cap) => {
        const key = cap.category || "uncategorized";
        (acc[key] ??= []).push(cap);
        return acc;
      },
      {},
    );
  }, [agent]);

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-[480px] sm:max-w-[480px]">
        {agent && (
          <>
            <SheetHeader>
              <SheetTitle className="flex items-center gap-2">
                {agent.name}
                {agent.origin === "a2a" && (
                  <Badge variant="outline">A2A</Badge>
                )}
              </SheetTitle>
              <SheetDescription>
                {agent.type} · {agent.version}
              </SheetDescription>
            </SheetHeader>

            <Tabs defaultValue="info" className="px-4">
              <TabsList>
                <TabsTrigger value="info">Info</TabsTrigger>
                <TabsTrigger value="capabilities">Capabilities</TabsTrigger>
                <TabsTrigger value="resources">Resources</TabsTrigger>
                <TabsTrigger value="events">Live Events</TabsTrigger>
              </TabsList>

              <TabsContent value="info" className="space-y-2 text-sm">
                <Row label="ID" value={agent.id} mono />
                <Row label="Status" value={<AgentStatusBadge status={agent.status} />} />
                <Row label="Node" value={agent.node_id} mono />
                <Row label="Registered" value={agent.registered_at} />
                <Row
                  label="Last heartbeat"
                  value={
                    agent.last_heartbeat
                      ? formatRelativeTime(
                          new Date(agent.last_heartbeat).getTime() *
                            1_000_000,
                        )
                      : "—"
                  }
                />
                {Object.keys(agent.labels).length > 0 && (
                  <div>
                    <div className="text-xs uppercase text-muted-foreground">
                      Labels
                    </div>
                    <div className="mt-1 flex flex-wrap gap-1">
                      {Object.entries(agent.labels).map(([k, v]) => (
                        <Badge key={k} variant="outline" className="text-xs">
                          {k}={v}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}
              </TabsContent>

              <TabsContent value="capabilities">
                <ScrollArea className="h-[60vh] pr-2">
                  {Object.entries(capsByCategory).map(([cat, caps]) => (
                    <div key={cat} className="mb-3">
                      <div className="text-xs uppercase text-muted-foreground">
                        {cat}
                      </div>
                      <ul className="mt-1 space-y-1">
                        {caps.map((c) => (
                          <li
                            key={`${c.name}@${c.version}`}
                            className="rounded border p-2 text-sm"
                          >
                            <div className="font-medium">
                              {c.name}{" "}
                              <span className="text-xs text-muted-foreground">
                                v{c.version}
                              </span>
                            </div>
                            {c.description && (
                              <div className="text-xs text-muted-foreground">
                                {c.description}
                              </div>
                            )}
                          </li>
                        ))}
                      </ul>
                    </div>
                  ))}
                  {agent.capabilities.length === 0 && (
                    <div className="text-sm text-muted-foreground">
                      No capabilities reported.
                    </div>
                  )}
                </ScrollArea>
              </TabsContent>

              <TabsContent value="resources" className="space-y-2 text-sm">
                {agent.resources ? (
                  <>
                    <Row
                      label="CPU"
                      value={`${agent.resources.cpu_percent.toFixed(1)}%`}
                    />
                    <Row
                      label="Memory"
                      value={formatBytes(agent.resources.memory_bytes)}
                    />
                    <Row
                      label="Pending events"
                      value={agent.resources.pending_events.toString()}
                    />
                  </>
                ) : (
                  <div className="text-muted-foreground">
                    No resource metrics reported.
                  </div>
                )}
              </TabsContent>

              <TabsContent value="events">
                <ScrollArea className="h-[60vh] pr-2">
                  {liveEvents.length === 0 ? (
                    <div className="text-sm text-muted-foreground">
                      Waiting for events…
                    </div>
                  ) : (
                    <ul className="space-y-1">
                      {liveEvents.map((ev) => (
                        <li
                          key={`${ev.stream}-${ev.sequence}`}
                          className="rounded border p-2 text-xs"
                        >
                          <div className="font-mono">{ev.type}</div>
                          <div className="text-muted-foreground">
                            {ev.source_agent}
                            {ev.target_agent ? ` → ${ev.target_agent}` : ""}
                          </div>
                        </li>
                      ))}
                    </ul>
                  )}
                </ScrollArea>
              </TabsContent>
            </Tabs>
          </>
        )}
      </SheetContent>
    </Sheet>
  );
}

function Row({
  label,
  value,
  mono,
}: {
  label: string;
  value: React.ReactNode;
  mono?: boolean;
}) {
  return (
    <div className="flex items-start justify-between gap-2">
      <span className="text-xs uppercase text-muted-foreground">{label}</span>
      <span className={mono ? "font-mono text-xs break-all" : "text-sm"}>
        {value}
      </span>
    </div>
  );
}
