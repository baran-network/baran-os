"use client";

import { useMemo, useState } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { AgentStatusBadge } from "./agent-status-badge";
import { formatRelativeTime } from "@/lib/utils";
import type { Agent } from "@/lib/types";

type SortKey = "name" | "type" | "status" | "capabilities" | "last_heartbeat";

interface Props {
  agents: Agent[];
  onSelect: (agent: Agent) => void;
  selectedId?: string | null;
}

export function AgentTable({ agents, onSelect, selectedId }: Props) {
  const [sortKey, setSortKey] = useState<SortKey>("name");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const sorted = useMemo(() => {
    const arr = [...agents];
    arr.sort((a, b) => {
      let av: string | number = "";
      let bv: string | number = "";
      switch (sortKey) {
        case "name":
          av = a.name;
          bv = b.name;
          break;
        case "type":
          av = a.type;
          bv = b.type;
          break;
        case "status":
          av = a.status;
          bv = b.status;
          break;
        case "capabilities":
          av = a.capabilities.length;
          bv = b.capabilities.length;
          break;
        case "last_heartbeat":
          av = a.last_heartbeat ?? "";
          bv = b.last_heartbeat ?? "";
          break;
      }
      if (av < bv) return sortDir === "asc" ? -1 : 1;
      if (av > bv) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
    return arr;
  }, [agents, sortKey, sortDir]);

  const toggleSort = (k: SortKey) => {
    if (sortKey === k) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(k);
      setSortDir("asc");
    }
  };

  const renderSortHead = (k: SortKey, label: string) => (
    <TableHead
      onClick={() => toggleSort(k)}
      className="cursor-pointer select-none"
    >
      {label}
      {sortKey === k && (
        <span className="ml-1 text-xs">{sortDir === "asc" ? "▲" : "▼"}</span>
      )}
    </TableHead>
  );

  if (agents.length === 0) {
    return (
      <div className="rounded-md border p-8 text-center text-sm text-muted-foreground">
        No agents match the current filters.
      </div>
    );
  }

  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            {renderSortHead("name", "Name")}
            {renderSortHead("type", "Type")}
            {renderSortHead("status", "Status")}
            {renderSortHead("capabilities", "Capabilities")}
            {renderSortHead("last_heartbeat", "Last heartbeat")}
          </TableRow>
        </TableHeader>
        <TableBody>
          {sorted.map((agent) => (
            <TableRow
              key={agent.id}
              onClick={() => onSelect(agent)}
              data-selected={selectedId === agent.id || undefined}
              className="cursor-pointer hover:bg-muted/50 data-[selected]:bg-muted"
            >
              <TableCell className="font-medium">
                <div className="flex items-center gap-2">
                  {agent.name}
                  {agent.origin === "a2a" && (
                    <Badge variant="outline" className="text-xs">
                      A2A
                    </Badge>
                  )}
                </div>
              </TableCell>
              <TableCell className="text-muted-foreground">{agent.type}</TableCell>
              <TableCell>
                <AgentStatusBadge status={agent.status} />
              </TableCell>
              <TableCell className="text-muted-foreground">
                {agent.capabilities.length}
              </TableCell>
              <TableCell className="text-muted-foreground">
                {agent.last_heartbeat
                  ? formatRelativeTime(
                      new Date(agent.last_heartbeat).getTime() * 1_000_000,
                    )
                  : "—"}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
