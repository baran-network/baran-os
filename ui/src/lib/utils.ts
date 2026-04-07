import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

// ---------------------------------------------------------------------------
// Timestamp formatting (Unix nanoseconds → human-readable)
// ---------------------------------------------------------------------------

/** Convert Unix nanoseconds to a Date. */
export function nanoToDate(nanos: number): Date {
  return new Date(nanos / 1_000_000);
}

/** Format Unix nanoseconds as ISO 8601 string. */
export function formatTimestampISO(nanos: number): string {
  return nanoToDate(nanos).toISOString();
}

/** Format Unix nanoseconds as locale time (HH:MM:SS). */
export function formatTimestampTime(nanos: number): string {
  return nanoToDate(nanos).toLocaleTimeString();
}

/** Format Unix nanoseconds as relative time (e.g. "2s ago", "5m ago"). */
export function formatRelativeTime(nanos: number): string {
  const diffMs = Date.now() - nanos / 1_000_000;
  if (diffMs < 0) return "just now";

  const seconds = Math.floor(diffMs / 1_000);
  if (seconds < 60) return `${seconds}s ago`;

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;

  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;

  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

// ---------------------------------------------------------------------------
// Byte formatting
// ---------------------------------------------------------------------------

const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB"];

export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(i === 0 ? 0 : 1)} ${BYTE_UNITS[i]}`;
}

// ---------------------------------------------------------------------------
// Duration formatting
// ---------------------------------------------------------------------------

/** Format milliseconds as human-readable duration. */
export function formatDuration(ms: number): string {
  if (ms < 1_000) return `${ms}ms`;
  const seconds = ms / 1_000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  const remainSec = Math.floor(seconds % 60);
  if (minutes < 60) return `${minutes}m ${remainSec}s`;
  const hours = Math.floor(minutes / 60);
  const remainMin = minutes % 60;
  return `${hours}h ${remainMin}m`;
}

// ---------------------------------------------------------------------------
// Dot-notation parsing
// ---------------------------------------------------------------------------

/** Extract the category (first segment) from a dot-notation string. */
export function dotCategory(dotNotation: string): string {
  return dotNotation.split(".")[0];
}

/** Extract the action (everything after the first dot). */
export function dotAction(dotNotation: string): string {
  const parts = dotNotation.split(".");
  return parts.slice(1).join(".");
}

/** Check if a dot-notation string matches a prefix (e.g., "workflow.*"). */
export function dotMatches(eventType: string, pattern: string): boolean {
  if (pattern.endsWith(".*")) {
    const prefix = pattern.slice(0, -2);
    return eventType === prefix || eventType.startsWith(prefix + ".");
  }
  return eventType === pattern;
}
