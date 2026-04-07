"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { injectEvent } from "@/lib/api";

// ---------------------------------------------------------------------------
// InjectionForm — manually inject a synthetic event into the bus
// ---------------------------------------------------------------------------

const COMMON_TYPES = [
  "agent.health.ping",
  "agent.health.pong",
  "agent.error",
  "workflow.start",
  "workflow.step.assign",
  "workflow.step.complete",
  "human.decision.request",
  "human.decision.response",
];

export function InjectionForm() {
  const [type, setType] = useState("");
  const [sourceAgent, setSourceAgent] = useState("");
  const [targetAgent, setTargetAgent] = useState("");
  const [workflowId, setWorkflowId] = useState("");
  const [payload, setPayload] = useState("{}");
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setResult(null);

    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(payload);
    } catch {
      setError("Payload must be valid JSON");
      return;
    }

    if (!type || !sourceAgent) {
      setError("Type and source agent are required");
      return;
    }

    setSubmitting(true);
    try {
      await injectEvent({
        type,
        source_agent: sourceAgent,
        target_agent: targetAgent || undefined,
        workflow_id: workflowId || undefined,
        payload: parsed,
      });
      setResult("Event injected.");
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="flex flex-col gap-3 rounded-lg border bg-card p-4">
      <div className="grid gap-3 md:grid-cols-2">
        <div>
          <label className="text-xs text-muted-foreground">Event type</label>
          <Input
            value={type}
            onChange={(e) => setType(e.target.value)}
            list="event-types"
            placeholder="agent.health.ping"
          />
          <datalist id="event-types">
            {COMMON_TYPES.map((t) => (
              <option key={t} value={t} />
            ))}
          </datalist>
        </div>
        <div>
          <label className="text-xs text-muted-foreground">Source agent</label>
          <Input
            value={sourceAgent}
            onChange={(e) => setSourceAgent(e.target.value)}
            placeholder="agent-id"
          />
        </div>
        <div>
          <label className="text-xs text-muted-foreground">
            Target agent (optional)
          </label>
          <Input
            value={targetAgent}
            onChange={(e) => setTargetAgent(e.target.value)}
            placeholder="agent-id"
          />
        </div>
        <div>
          <label className="text-xs text-muted-foreground">
            Workflow ID (optional)
          </label>
          <Input
            value={workflowId}
            onChange={(e) => setWorkflowId(e.target.value)}
            placeholder="uuid"
          />
        </div>
      </div>
      <div>
        <label className="text-xs text-muted-foreground">Payload (JSON)</label>
        <Textarea
          value={payload}
          onChange={(e) => setPayload(e.target.value)}
          rows={6}
          className="font-mono text-xs"
        />
      </div>
      <div className="flex items-center gap-2">
        <Button type="submit" disabled={submitting}>
          {submitting ? "Injecting…" : "Inject event"}
        </Button>
        {result && <span className="text-xs text-emerald-600">{result}</span>}
        {error && <span className="text-xs text-destructive">{error}</span>}
      </div>
    </form>
  );
}
