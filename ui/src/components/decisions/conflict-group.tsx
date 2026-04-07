"use client";

import { Badge } from "@/components/ui/badge";
import { DecisionCard } from "./decision-card";
import type { Decision } from "@/lib/types";

interface Props {
  conflictId: string;
  decisions: Decision[];
  onRespond: (
    decisionId: string,
    action: "approve" | "reject",
    comment?: string,
  ) => Promise<void>;
  readOnly?: boolean;
}

export function ConflictGroup({ conflictId, decisions, onRespond, readOnly }: Props) {
  return (
    <div className="rounded-lg border border-destructive/40 bg-destructive/5 p-3 space-y-3">
      <div className="flex items-center gap-2">
        <Badge variant="destructive" className="text-xs">
          Conflict
        </Badge>
        <span className="text-xs font-mono text-muted-foreground">{conflictId}</span>
        <span className="text-xs text-muted-foreground ml-auto">
          {decisions.length} decision{decisions.length !== 1 ? "s" : ""}
        </span>
      </div>
      <div className="space-y-2">
        {decisions.map((d) => (
          <DecisionCard
            key={d.decision_id}
            decision={d}
            onRespond={onRespond}
            readOnly={readOnly}
          />
        ))}
      </div>
    </div>
  );
}
