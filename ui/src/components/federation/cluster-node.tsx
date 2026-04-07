"use client";

import { Handle, Position, type NodeProps, type Node } from "@xyflow/react";
import type { ClusterNodeData } from "@/hooks/use-federation";
import { Badge } from "@/components/ui/badge";

const STATUS_COLOR: Record<string, string> = {
  active: "bg-green-500/15 text-green-700 dark:text-green-400 border-green-500/30",
  unhealthy:
    "bg-yellow-500/15 text-yellow-700 dark:text-yellow-400 border-yellow-500/30",
  dead: "bg-red-500/15 text-red-700 dark:text-red-400 border-red-500/30",
  syncing:
    "bg-blue-500/15 text-blue-700 dark:text-blue-400 border-blue-500/30",
  disconnected:
    "bg-gray-500/15 text-gray-700 dark:text-gray-400 border-gray-500/30",
};

export function ClusterNode({
  data,
}: NodeProps<Node<ClusterNodeData>>) {
  const { cluster } = data;
  const colorClass = STATUS_COLOR[cluster.status] ?? STATUS_COLOR.disconnected;

  return (
    <div className="rounded-lg border bg-card text-card-foreground shadow-sm min-w-[200px]">
      <Handle type="target" position={Position.Top} />
      <div className="p-3 space-y-2">
        <div className="flex items-center justify-between gap-2">
          <span className="font-medium text-sm truncate">{cluster.name}</span>
          <Badge className={`text-xs ${colorClass}`}>{cluster.status}</Badge>
        </div>
        <div className="text-xs text-muted-foreground space-y-0.5">
          <div>{cluster.address}</div>
          <div>
            {cluster.agent_count} agents · {cluster.capabilities?.length ?? 0}{" "}
            caps
          </div>
        </div>
      </div>
      <Handle type="source" position={Position.Bottom} />
    </div>
  );
}
