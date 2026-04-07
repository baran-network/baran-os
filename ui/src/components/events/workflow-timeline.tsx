"use client";

import { useEffect, useState } from "react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { ScrollArea, ScrollBar } from "@/components/ui/scroll-area";
import { fetchWorkflows, fetchWorkflow } from "@/lib/api";
import { formatDuration } from "@/lib/utils";
import { cn } from "@/lib/utils";
import type { Workflow, WorkflowStep, WorkflowStepStatus } from "@/lib/types";

// ---------------------------------------------------------------------------
// WorkflowSelector
// ---------------------------------------------------------------------------

interface WorkflowSelectorProps {
  selectedId: string | null;
  onSelect: (id: string | null) => void;
}

export function WorkflowSelector({
  selectedId,
  onSelect,
}: WorkflowSelectorProps) {
  const [workflows, setWorkflows] = useState<Workflow[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    void fetchWorkflows({ limit: 50 })
      .then((r) => setWorkflows(r.workflows))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <Select
      value={selectedId ?? undefined}
      onValueChange={(v) => onSelect(v || null)}
      disabled={loading}
    >
      <SelectTrigger className="h-8 w-[300px] text-sm">
        <SelectValue placeholder="Select a workflow…" />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="">None</SelectItem>
        {workflows.map((wf) => (
          <SelectItem key={wf.id} value={wf.id}>
            <span className="font-mono text-xs">{wf.id.slice(0, 8)}</span>
            <span className="ml-2 text-muted-foreground">
              {wf.name} — {wf.status}
            </span>
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

// ---------------------------------------------------------------------------
// Step status styling
// ---------------------------------------------------------------------------

const STEP_STATUS_CLASSES: Record<WorkflowStepStatus, string> = {
  pending: "bg-muted border-muted-foreground/30 text-muted-foreground",
  running: "bg-blue-100 border-blue-400 text-blue-700 dark:bg-blue-950 dark:text-blue-300",
  completed: "bg-green-100 border-green-400 text-green-700 dark:bg-green-950 dark:text-green-300",
  failed: "bg-red-100 border-red-400 text-red-700 dark:bg-red-950 dark:text-red-300",
};

const STEP_STATUS_DOT: Record<WorkflowStepStatus, string> = {
  pending: "bg-muted-foreground/40",
  running: "bg-blue-500 animate-pulse",
  completed: "bg-green-500",
  failed: "bg-red-500",
};

// ---------------------------------------------------------------------------
// Step card
// ---------------------------------------------------------------------------

interface StepCardProps {
  step: WorkflowStep;
  selected: boolean;
  onClick: () => void;
}

function StepCard({ step, selected, onClick }: StepCardProps) {
  const classes = STEP_STATUS_CLASSES[step.status];
  const dotClass = STEP_STATUS_DOT[step.status];

  return (
    <button
      onClick={onClick}
      className={cn(
        "flex shrink-0 flex-col gap-1 rounded border px-3 py-2 text-left transition-all w-36",
        classes,
        selected && "ring-2 ring-ring ring-offset-1",
      )}
    >
      <div className="flex items-center gap-1.5">
        <span className={cn("h-2 w-2 rounded-full shrink-0", dotClass)} />
        <span className="truncate text-xs font-semibold">{step.name}</span>
      </div>
      <span className="text-[10px] truncate opacity-80">{step.capability}</span>
      {step.duration_ms !== null && (
        <span className="text-[10px] tabular-nums opacity-70">
          {formatDuration(step.duration_ms)}
        </span>
      )}
    </button>
  );
}

// ---------------------------------------------------------------------------
// WorkflowTimeline
// ---------------------------------------------------------------------------

interface WorkflowTimelineProps {
  workflowId: string;
}

export function WorkflowTimeline({ workflowId }: WorkflowTimelineProps) {
  const [workflow, setWorkflow] = useState<Workflow | null>(null);
  const [selectedStep, setSelectedStep] = useState<WorkflowStep | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setSelectedStep(null);
    void fetchWorkflow(workflowId)
      .then(setWorkflow)
      .catch((e: unknown) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
      .finally(() => setLoading(false));
  }, [workflowId]);

  if (loading) {
    return (
      <div className="py-6 text-center text-sm text-muted-foreground">
        Loading workflow…
      </div>
    );
  }

  if (error || !workflow) {
    return (
      <div className="py-6 text-center text-sm text-destructive">
        {error ?? "Workflow not found"}
      </div>
    );
  }

  const steps = workflow.steps ?? [];

  return (
    <div className="flex flex-col gap-4">
      {/* Header */}
      <div className="flex items-center gap-3">
        <span className="font-mono text-xs text-muted-foreground">
          {workflow.id}
        </span>
        <Badge variant="outline">{workflow.status}</Badge>
        <span className="text-sm font-medium">{workflow.name}</span>
      </div>

      {/* Horizontal step bar */}
      <ScrollArea className="w-full">
        <div className="flex items-center gap-2 pb-3">
          {steps.map((step, i) => (
            <div key={step.index} className="flex items-center">
              <StepCard
                step={step}
                selected={selectedStep?.index === step.index}
                onClick={() =>
                  setSelectedStep(
                    selectedStep?.index === step.index ? null : step,
                  )
                }
              />
              {i < steps.length - 1 && (
                <div className="mx-1 h-px w-6 shrink-0 bg-border" />
              )}
            </div>
          ))}
        </div>
        <ScrollBar orientation="horizontal" />
      </ScrollArea>

      {/* Step detail */}
      {selectedStep && (
        <div className="rounded-lg border border-border bg-card p-3 text-sm">
          <div className="mb-2 flex items-center gap-2">
            <span className="font-semibold">
              Step {selectedStep.index + 1}: {selectedStep.name}
            </span>
            <Badge
              variant="outline"
              className={cn(
                "text-[10px]",
                STEP_STATUS_CLASSES[selectedStep.status],
              )}
            >
              {selectedStep.status}
            </Badge>
          </div>
          <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
            <dt className="text-muted-foreground">Capability</dt>
            <dd className="font-mono">{selectedStep.capability}</dd>

            {selectedStep.assigned_agent && (
              <>
                <dt className="text-muted-foreground">Agent</dt>
                <dd className="font-mono">{selectedStep.assigned_agent}</dd>
              </>
            )}

            {selectedStep.duration_ms !== null && (
              <>
                <dt className="text-muted-foreground">Duration</dt>
                <dd>{formatDuration(selectedStep.duration_ms)}</dd>
              </>
            )}

            {selectedStep.started_at && (
              <>
                <dt className="text-muted-foreground">Started</dt>
                <dd className="font-mono text-[11px]">
                  {selectedStep.started_at}
                </dd>
              </>
            )}

            {selectedStep.completed_at && (
              <>
                <dt className="text-muted-foreground">Completed</dt>
                <dd className="font-mono text-[11px]">
                  {selectedStep.completed_at}
                </dd>
              </>
            )}
          </dl>

          {selectedStep.result && (
            <div className="mt-2">
              <p className="mb-1 text-xs text-muted-foreground">Result</p>
              <pre className="overflow-x-auto rounded bg-muted/60 p-2 text-[11px]">
                {JSON.stringify(selectedStep.result, null, 2)}
              </pre>
            </div>
          )}
        </div>
      )}

      {steps.length === 0 && (
        <p className="text-sm text-muted-foreground">No steps available.</p>
      )}
    </div>
  );
}
