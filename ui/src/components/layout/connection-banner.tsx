"use client";

import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import type { ConnectionState } from "@/lib/types";

// -- Connection Indicator (green/yellow/red dot) ----------------------------

interface ConnectionIndicatorProps {
  state: ConnectionState;
  className?: string;
}

const indicatorColors: Record<ConnectionState, string> = {
  connected: "bg-green-500",
  connecting: "bg-yellow-500 animate-pulse",
  disconnected: "bg-red-500",
};

const indicatorLabels: Record<ConnectionState, string> = {
  connected: "Connected",
  connecting: "Connecting…",
  disconnected: "Disconnected",
};

export function ConnectionIndicator({
  state,
  className,
}: ConnectionIndicatorProps) {
  return (
    <div className={cn("flex items-center gap-2 text-sm", className)}>
      <span
        className={cn("h-2.5 w-2.5 rounded-full", indicatorColors[state])}
      />
      <span className="text-muted-foreground">{indicatorLabels[state]}</span>
    </div>
  );
}

// -- Connection Banner (error banner with retry) ----------------------------

interface ConnectionBannerProps {
  state: ConnectionState;
  onRetry: () => void;
}

export function ConnectionBanner({ state, onRetry }: ConnectionBannerProps) {
  if (state === "connected") return null;

  const isConnecting = state === "connecting";

  return (
    <div
      className={cn(
        "flex items-center justify-between px-4 py-2 text-sm",
        isConnecting
          ? "bg-yellow-500/10 text-yellow-700 dark:text-yellow-400"
          : "bg-red-500/10 text-red-700 dark:text-red-400",
      )}
    >
      <span>
        {isConnecting
          ? "Reconnecting to Baran OS runtime…"
          : "Connection to Baran OS runtime lost."}
      </span>
      {!isConnecting && (
        <Button variant="outline" size="sm" onClick={onRetry}>
          Retry
        </Button>
      )}
    </div>
  );
}
