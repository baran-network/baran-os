"use client";

import { Button } from "@/components/ui/button";

export default function ErrorBoundary({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div className="flex flex-col items-center justify-center h-64 gap-3">
      <p className="text-sm font-medium text-destructive">
        Failed to load simulator
      </p>
      <p className="text-xs text-muted-foreground max-w-md text-center">
        {error.message}
      </p>
      <Button size="sm" variant="outline" onClick={reset}>
        Retry
      </Button>
    </div>
  );
}
