"use client";

import type { Cluster } from "@/lib/types";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "@/components/ui/sheet";
import { Badge } from "@/components/ui/badge";
import { formatTimestampISO, formatRelativeTime } from "@/lib/utils";

export interface ClusterDetailProps {
  cluster: Cluster | null;
  onClose: () => void;
}

export function ClusterDetail({ cluster, onClose }: ClusterDetailProps) {
  return (
    <Sheet open={cluster !== null} onOpenChange={(o) => !o && onClose()}>
      <SheetContent className="sm:max-w-[480px]">
        {cluster && (
          <>
            <SheetHeader>
              <SheetTitle>{cluster.name}</SheetTitle>
              <SheetDescription>
                {cluster.address} · v{cluster.version || "?"}
              </SheetDescription>
            </SheetHeader>

            <div className="p-4 space-y-4">
              <div className="flex items-center gap-2">
                <Badge>{cluster.status}</Badge>
                {cluster.missed_heartbeats > 0 && (
                  <Badge variant="outline">
                    missed: {cluster.missed_heartbeats}
                  </Badge>
                )}
              </div>

              <section>
                <h3 className="text-sm font-medium mb-2">Stats</h3>
                <dl className="grid grid-cols-2 gap-2 text-sm">
                  <dt className="text-muted-foreground">Agents</dt>
                  <dd>{cluster.agent_count}</dd>
                  <dt className="text-muted-foreground">Workflows</dt>
                  <dd>{cluster.workflow_count}</dd>
                  <dt className="text-muted-foreground">Capabilities</dt>
                  <dd>{cluster.capabilities?.length ?? 0}</dd>
                  <dt className="text-muted-foreground">Joined</dt>
                  <dd>{formatTimestampISO(cluster.joined_at)}</dd>
                  <dt className="text-muted-foreground">Last seen</dt>
                  <dd>{formatRelativeTime(cluster.last_seen)}</dd>
                </dl>
              </section>

              {cluster.capabilities && cluster.capabilities.length > 0 && (
                <section>
                  <h3 className="text-sm font-medium mb-2">Capabilities</h3>
                  <div className="flex flex-wrap gap-1">
                    {cluster.capabilities.map((c) => (
                      <Badge key={c} variant="outline" className="text-xs">
                        {c}
                      </Badge>
                    ))}
                  </div>
                </section>
              )}
            </div>
          </>
        )}
      </SheetContent>
    </Sheet>
  );
}
