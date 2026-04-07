"use client";

import { useState } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ReplayControls } from "@/components/simulator/replay-controls";
import { ReplayTimeline } from "@/components/simulator/replay-timeline";
import { ScenarioList } from "@/components/simulator/scenario-list";
import { ScenarioProgress } from "@/components/simulator/scenario-progress";
import { InjectionForm } from "@/components/simulator/injection-form";
import { SimulationStream } from "@/components/simulator/simulation-stream";
import type { ReplaySession, ScenarioSession } from "@/lib/types";

export default function SimulatorPage() {
  const [replaySession, setReplaySession] = useState<ReplaySession | null>(null);
  const [scenarioSession, setScenarioSession] =
    useState<ScenarioSession | null>(null);

  return (
    <div className="flex h-full flex-col gap-4 p-6">
      <div className="flex items-baseline justify-between">
        <h1 className="text-2xl font-semibold">Simulator</h1>
        <p className="text-sm text-muted-foreground">
          Replay workflows, run scenarios, inject events.
        </p>
      </div>

      <div className="grid flex-1 gap-4 overflow-hidden lg:grid-cols-[1fr_minmax(0,420px)]">
        <Tabs defaultValue="replay" className="flex min-h-0 flex-col">
          <TabsList>
            <TabsTrigger value="replay">Event Replay</TabsTrigger>
            <TabsTrigger value="scenarios">Scenarios</TabsTrigger>
            <TabsTrigger value="inject">Manual Injection</TabsTrigger>
          </TabsList>
          <TabsContent value="replay" className="flex flex-col gap-3">
            <ReplayControls onSession={setReplaySession} />
            <ReplayTimeline session={replaySession} />
          </TabsContent>
          <TabsContent value="scenarios" className="flex flex-col gap-3">
            <ScenarioList onSessionStarted={setScenarioSession} />
            <ScenarioProgress session={scenarioSession} />
          </TabsContent>
          <TabsContent value="inject">
            <InjectionForm />
          </TabsContent>
        </Tabs>

        <div className="min-h-0 overflow-hidden">
          <SimulationStream />
        </div>
      </div>
    </div>
  );
}
