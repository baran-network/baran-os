"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { fetchScenarios, startScenario } from "@/lib/api";
import type { Scenario, ScenarioSession } from "@/lib/types";

// ---------------------------------------------------------------------------
// ScenarioList — cards of registered scenarios with run button
// ---------------------------------------------------------------------------

interface ScenarioListProps {
  onSessionStarted?: (session: ScenarioSession) => void;
}

export function ScenarioList({ onSessionStarted }: ScenarioListProps) {
  const [scenarios, setScenarios] = useState<Scenario[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState<string | null>(null);

  useEffect(() => {
    fetchScenarios()
      .then(setScenarios)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const handleRun = async (id: string) => {
    setRunning(id);
    try {
      const session = await startScenario(id);
      onSessionStarted?.(session);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setRunning(null);
    }
  };

  if (loading) {
    return (
      <div className="text-sm text-muted-foreground">Loading scenarios…</div>
    );
  }

  if (error) {
    return <div className="text-sm text-destructive">{error}</div>;
  }

  if (scenarios.length === 0) {
    return (
      <div className="rounded-lg border bg-card p-6 text-center text-sm text-muted-foreground">
        No scenarios registered. Use{" "}
        <code className="font-mono text-xs">POST /api/simulation/scenarios</code>{" "}
        to register one.
      </div>
    );
  }

  return (
    <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
      {scenarios.map((s) => {
        const stepCount =
          typeof s.steps === "number" ? s.steps : s.steps.length;
        return (
          <Card key={s.id} className="flex flex-col gap-2 p-4">
            <div className="flex items-start justify-between gap-2">
              <div className="font-semibold">{s.name}</div>
              <Badge variant="outline">{stepCount} steps</Badge>
            </div>
            {s.description && (
              <div className="text-xs text-muted-foreground line-clamp-3">
                {s.description}
              </div>
            )}
            <div className="mt-auto flex justify-end pt-2">
              <Button
                size="sm"
                onClick={() => handleRun(s.id)}
                disabled={running === s.id}
              >
                {running === s.id ? "Starting…" : "Run scenario"}
              </Button>
            </div>
          </Card>
        );
      })}
    </div>
  );
}
