"use client";

import { useMemo } from "react";
import { DecisionCard } from "./decision-card";
import { ConflictGroup } from "./conflict-group";
import type { Decision } from "@/lib/types";

interface Props {
  decisions: Decision[];
  onRespond: (
    decisionId: string,
    action: "approve" | "reject",
    comment?: string,
  ) => Promise<void>;
  readOnly?: boolean;
}

interface Group {
  conflictId: string | null;
  decisions: Decision[];
}

export function DecisionList({ decisions, onRespond, readOnly = false }: Props) {
  const groups = useMemo<Group[]>(() => {
    // Decisions with no conflict IDs → individual cards
    // Decisions sharing a conflict ID → grouped together
    const conflictMap = new Map<string, Decision[]>();
    const standalone: Decision[] = [];

    for (const d of decisions) {
      if (!d.conflict_ids || d.conflict_ids.length === 0) {
        standalone.push(d);
      } else {
        // Use first conflict_id as group key
        const key = d.conflict_ids[0];
        if (!conflictMap.has(key)) conflictMap.set(key, []);
        conflictMap.get(key)!.push(d);
      }
    }

    const result: Group[] = [];

    // Conflict groups first
    for (const [conflictId, group] of conflictMap.entries()) {
      result.push({ conflictId, decisions: group });
    }

    // Then standalone decisions
    for (const d of standalone) {
      result.push({ conflictId: null, decisions: [d] });
    }

    return result;
  }, [decisions]);

  if (decisions.length === 0) {
    return (
      <div className="flex items-center justify-center h-40 text-sm text-muted-foreground">
        {readOnly ? "No resolved decisions." : "No pending decisions."}
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {groups.map((g) => {
        if (g.conflictId !== null) {
          return (
            <ConflictGroup
              key={g.conflictId}
              conflictId={g.conflictId}
              decisions={g.decisions}
              onRespond={onRespond}
              readOnly={readOnly}
            />
          );
        }
        return (
          <DecisionCard
            key={g.decisions[0].decision_id}
            decision={g.decisions[0]}
            onRespond={onRespond}
            readOnly={readOnly}
          />
        );
      })}
    </div>
  );
}

// Subcomponent for resolved/history view
interface HistoryProps {
  decisions: Decision[];
}

export function DecisionHistory({ decisions }: HistoryProps) {
  if (decisions.length === 0) {
    return (
      <div className="flex items-center justify-center h-40 text-sm text-muted-foreground">
        No resolved decisions.
      </div>
    );
  }

  // Sort history by responded_at descending
  const sorted = [...decisions].sort((a, b) => {
    const ta = a.responded_at ? new Date(a.responded_at).getTime() : 0;
    const tb = b.responded_at ? new Date(b.responded_at).getTime() : 0;
    return tb - ta;
  });

  return (
    <div className="space-y-3">
      {sorted.map((d) => (
        <DecisionCard
          key={d.decision_id}
          decision={d}
          onRespond={async () => {}}
          readOnly
        />
      ))}
    </div>
  );
}
