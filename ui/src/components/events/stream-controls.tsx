"use client";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Pause, Play, Trash2, Zap } from "lucide-react";

// ---------------------------------------------------------------------------
// StreamControls — play/pause toggle, buffered count, high-traffic indicator
// ---------------------------------------------------------------------------

interface StreamControlsProps {
  paused: boolean;
  bufferedCount: number;
  isHighTraffic: boolean;
  onPause: () => void;
  onResume: () => void;
  onClear: () => void;
}

export function StreamControls({
  paused,
  bufferedCount,
  isHighTraffic,
  onPause,
  onResume,
  onClear,
}: StreamControlsProps) {
  return (
    <div className="flex items-center gap-2">
      {/* High-traffic indicator */}
      {isHighTraffic && (
        <Badge
          variant="outline"
          className="gap-1 border-amber-400 bg-amber-50 text-amber-700 dark:bg-amber-950 dark:text-amber-300"
        >
          <Zap className="h-3 w-3" />
          High traffic
        </Badge>
      )}

      {/* Buffered count (only when paused) */}
      {paused && bufferedCount > 0 && (
        <Badge variant="secondary" className="text-xs">
          {bufferedCount.toLocaleString()} buffered
        </Badge>
      )}

      {/* Play / Pause */}
      {paused ? (
        <Button
          size="sm"
          variant="default"
          onClick={onResume}
          className="gap-1"
        >
          <Play className="h-3.5 w-3.5" />
          Resume
        </Button>
      ) : (
        <Button
          size="sm"
          variant="outline"
          onClick={onPause}
          className="gap-1"
        >
          <Pause className="h-3.5 w-3.5" />
          Pause
        </Button>
      )}

      {/* Clear */}
      <Button
        size="sm"
        variant="ghost"
        onClick={onClear}
        className="gap-1 text-muted-foreground"
      >
        <Trash2 className="h-3.5 w-3.5" />
        Clear
      </Button>
    </div>
  );
}
