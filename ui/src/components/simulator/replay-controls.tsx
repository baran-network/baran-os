"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import {
  createReplaySession,
  fetchReplaySession,
  fetchWorkflows,
  stopReplaySession,
} from "@/lib/api";
import type { ReplaySession, Workflow } from "@/lib/types";

// ---------------------------------------------------------------------------
// ReplayControls — workflow picker, speed, start/stop, progress + SSE stream
// ---------------------------------------------------------------------------

const SPEEDS = [
  { label: "1x", value: 1 },
  { label: "2x", value: 2 },
  { label: "5x", value: 5 },
  { label: "10x", value: 10 },
  { label: "Max", value: 0 }, // 0 = no delay (max)
];

interface ReplayControlsProps {
  onSession?: (session: ReplaySession | null) => void;
}

export function ReplayControls({ onSession }: ReplayControlsProps) {
  const [workflows, setWorkflows] = useState<Workflow[]>([]);
  const [workflowId, setWorkflowId] = useState<string>("");
  const [speed, setSpeed] = useState<number>(1);
  const [session, setSession] = useState<ReplaySession | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchWorkflows({ limit: 100 })
      .then((res) => setWorkflows(res.workflows ?? []))
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => {
    onSession?.(session);
  }, [session, onSession]);

  // Poll session state while running (replay events flow via the global stream)
  useEffect(() => {
    if (!session || session.state !== "running") return;
    const id = setInterval(async () => {
      try {
        const fresh = await fetchReplaySession(session.id);
        setSession(fresh);
      } catch {
        // ignore
      }
    }, 1000);
    return () => clearInterval(id);
  }, [session?.id, session?.state]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleStart = async () => {
    if (!workflowId) return;
    setError(null);
    try {
      const s = await createReplaySession(workflowId, speed);
      setSession(s);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const handleStop = async () => {
    if (!session) return;
    try {
      await stopReplaySession(session.id);
      const fresh = await fetchReplaySession(session.id);
      setSession(fresh);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const isRunning = session?.state === "running";
  const progress =
    session && session.total_events > 0
      ? Math.round((session.replayed_events / session.total_events) * 100)
      : 0;

  return (
    <div className="flex flex-col gap-3 rounded-lg border bg-card p-4">
      <div className="flex flex-wrap items-end gap-2">
        <div className="flex-1 min-w-[240px]">
          <label className="text-xs text-muted-foreground">Workflow</label>
          <Select value={workflowId} onValueChange={(v) => setWorkflowId(v ?? "")}>
            <SelectTrigger>
              <SelectValue placeholder="Select a workflow…" />
            </SelectTrigger>
            <SelectContent>
              {workflows.map((w) => (
                <SelectItem key={w.id} value={w.id}>
                  {w.name} ({w.id.slice(0, 8)}…)
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div>
          <label className="text-xs text-muted-foreground">Speed</label>
          <Select
            value={String(speed)}
            onValueChange={(v) => setSpeed(Number(v ?? "1"))}
          >
            <SelectTrigger className="w-24">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {SPEEDS.map((s) => (
                <SelectItem key={s.value} value={String(s.value)}>
                  {s.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <Button
          onClick={handleStart}
          disabled={!workflowId || isRunning}
        >
          Start replay
        </Button>
        <Button
          onClick={handleStop}
          variant="secondary"
          disabled={!isRunning}
        >
          Stop
        </Button>
      </div>

      {session && (
        <div className="flex flex-col gap-1">
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <Badge variant="outline">{session.state}</Badge>
            <span>
              {session.replayed_events} / {session.total_events} events
            </span>
            <span className="ml-auto">{progress}%</span>
          </div>
          <div className="h-2 w-full overflow-hidden rounded bg-muted">
            <div
              className="h-full bg-pink-500 transition-all"
              style={{ width: `${progress}%` }}
            />
          </div>
        </div>
      )}

      {error && (
        <div className="text-xs text-destructive">{error}</div>
      )}
    </div>
  );
}
