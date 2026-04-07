"use client";

import { useState } from "react";
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { formatRelativeTime } from "@/lib/utils";
import type { Decision } from "@/lib/types";

interface Props {
  decision: Decision;
  onRespond: (
    decisionId: string,
    action: "approve" | "reject",
    comment?: string,
  ) => Promise<void>;
  readOnly?: boolean;
}

export function DecisionCard({ decision, onRespond, readOnly = false }: Props) {
  const [comment, setComment] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleRespond = async (action: "approve" | "reject") => {
    setSubmitting(true);
    setError(null);
    try {
      await onRespond(decision.decision_id, action, comment.trim() || undefined);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const isResolved = !!decision.action;

  return (
    <Card className="border border-border">
      <CardHeader className="pb-2">
        <div className="flex items-start justify-between gap-2">
          <CardTitle className="text-sm font-semibold leading-snug">
            {decision.prompt}
          </CardTitle>
          <div className="flex items-center gap-1 shrink-0">
            {decision.conflict_ids?.length > 0 && (
              <Badge variant="destructive" className="text-xs">
                conflict
              </Badge>
            )}
            {isResolved && (
              <Badge
                variant={decision.action === "approve" ? "default" : "secondary"}
                className="text-xs"
              >
                {decision.action}
              </Badge>
            )}
          </div>
        </div>

        <div className="text-xs text-muted-foreground space-y-0.5 mt-1">
          <div>
            Workflow:{" "}
            <span className="font-mono">{decision.workflow_id}</span>
          </div>
          <div>
            Step {decision.step_index}:{" "}
            <span className="font-medium">{decision.step_name}</span>
          </div>
          <div>
            Requested:{" "}
            {formatRelativeTime(new Date(decision.requested_at).getTime() * 1e6)}
          </div>
          {isResolved && decision.responded_at && (
            <div>
              Resolved:{" "}
              {formatRelativeTime(new Date(decision.responded_at).getTime() * 1e6)}
            </div>
          )}
        </div>
      </CardHeader>

      {decision.resource_ids?.length > 0 && (
        <CardContent className="pb-2 pt-0">
          <div className="text-xs text-muted-foreground mb-1">Resources</div>
          <div className="flex flex-wrap gap-1">
            {decision.resource_ids.map((rid) => (
              <Badge key={rid} variant="outline" className="text-xs font-mono">
                {rid}
              </Badge>
            ))}
          </div>
        </CardContent>
      )}

      {isResolved && decision.comment && (
        <CardContent className="pb-2 pt-0">
          <div className="text-xs text-muted-foreground mb-1">Comment</div>
          <p className="text-xs italic text-foreground/80">{decision.comment}</p>
        </CardContent>
      )}

      {!readOnly && !isResolved && (
        <CardFooter className="flex flex-col gap-2 pt-2">
          <textarea
            className="w-full text-xs rounded border border-input bg-background px-2 py-1 resize-none focus:outline-none focus:ring-1 focus:ring-ring"
            rows={2}
            placeholder="Optional comment…"
            value={comment}
            onChange={(e) => setComment(e.target.value)}
            disabled={submitting}
          />
          {error && <p className="text-xs text-destructive">{error}</p>}
          <div className="flex gap-2 w-full">
            <Button
              size="sm"
              className="flex-1"
              onClick={() => void handleRespond("approve")}
              disabled={submitting}
            >
              Approve
            </Button>
            <Button
              size="sm"
              variant="outline"
              className="flex-1 text-destructive border-destructive hover:bg-destructive/10"
              onClick={() => void handleRespond("reject")}
              disabled={submitting}
            >
              Reject
            </Button>
          </div>
        </CardFooter>
      )}
    </Card>
  );
}
