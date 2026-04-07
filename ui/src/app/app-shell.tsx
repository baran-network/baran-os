"use client";

import { useCallback, useState } from "react";
import { Sidebar } from "@/components/layout/sidebar";
import {
  ConnectionBanner,
  ConnectionIndicator,
} from "@/components/layout/connection-banner";
import { useConnection } from "@/hooks/use-connection";

export function AppShell({ children }: { children: React.ReactNode }) {
  const [pendingDecisions, setPendingDecisions] = useState(0);

  const handleEvent = useCallback(
    (event: { type: string }) => {
      // Track pending decision count from decision stream events
      if (event.type === "human.decision.request") {
        setPendingDecisions((c) => c + 1);
      } else if (event.type === "human.decision.response") {
        setPendingDecisions((c) => Math.max(0, c - 1));
      }
    },
    [],
  );

  const { state, reconnect } = useConnection({ onEvent: handleEvent });

  return (
    <div className="flex h-full">
      <Sidebar pendingDecisions={pendingDecisions} />
      <div className="flex flex-1 flex-col overflow-hidden">
        <header className="flex h-12 items-center justify-between border-b px-4">
          <span className="text-sm font-medium text-muted-foreground">
            Operator Dashboard
          </span>
          <ConnectionIndicator state={state} />
        </header>
        <ConnectionBanner state={state} onRetry={reconnect} />
        <main className="flex-1 overflow-auto p-4">{children}</main>
      </div>
    </div>
  );
}
