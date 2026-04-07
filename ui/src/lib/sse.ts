// ---------------------------------------------------------------------------
// Baran OS Operator UI — SSE Connection Manager
// ---------------------------------------------------------------------------

import type { ConnectionState, Event, SSEGapEvent, SSEOptions } from "./types";

const KEEPALIVE_TIMEOUT_MS = 45_000; // 3× the 15s server keepalive interval
const RECONNECT_DELAYS = [1_000, 2_000, 4_000, 8_000]; // exponential backoff, 8s cap

export class SSEManager {
  private es: EventSource | null = null;
  private keepaliveTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempt = 0;
  private lastEventId: string | undefined;
  private state: ConnectionState = "disconnected";
  private disposed = false;

  constructor(private opts: SSEOptions) {
    this.lastEventId = opts.lastEventId;
  }

  // -- Lifecycle ------------------------------------------------------------

  connect(): void {
    if (this.disposed) return;
    this.cleanup();
    this.setState("connecting");

    const url = this.buildUrl();
    this.es = new EventSource(url);

    this.es.onopen = () => {
      this.reconnectAttempt = 0;
      this.setState("connected");
      this.resetKeepalive();
    };

    this.es.onmessage = (msg) => {
      this.resetKeepalive();
      this.lastEventId = msg.lastEventId || this.lastEventId;

      try {
        const data = JSON.parse(msg.data) as Event;
        this.opts.onEvent?.(data);
      } catch {
        // non-JSON message (keepalive comment handled by browser)
      }
    };

    // Named event: gap
    this.es.addEventListener("gap", ((ev: MessageEvent) => {
      this.resetKeepalive();
      try {
        const gap = JSON.parse(ev.data) as SSEGapEvent;
        this.opts.onGap?.(gap);
      } catch {
        // ignore malformed gap
      }
    }) as EventListener);

    this.es.onerror = () => {
      this.setState("disconnected");
      this.opts.onError?.(new Error("SSE connection lost"));
      this.scheduleReconnect();
    };
  }

  disconnect(): void {
    this.disposed = true;
    this.cleanup();
    this.setState("disconnected");
  }

  getState(): ConnectionState {
    return this.state;
  }

  getLastEventId(): string | undefined {
    return this.lastEventId;
  }

  // -- Internals ------------------------------------------------------------

  private buildUrl(): string {
    const params = new URLSearchParams(this.opts.params);
    if (this.lastEventId) {
      params.set("last_event_id", this.lastEventId);
    }
    if (this.opts.token) {
      params.set("token", this.opts.token);
    }
    const qs = params.toString();
    return qs ? `${this.opts.url}?${qs}` : this.opts.url;
  }

  private setState(next: ConnectionState): void {
    if (this.state !== next) {
      this.state = next;
      this.opts.onStateChange?.(next);
    }
  }

  private resetKeepalive(): void {
    if (this.keepaliveTimer) clearTimeout(this.keepaliveTimer);
    this.keepaliveTimer = setTimeout(() => {
      // No data received within timeout — assume stale connection
      this.es?.close();
      this.setState("disconnected");
      this.scheduleReconnect();
    }, KEEPALIVE_TIMEOUT_MS);
  }

  private scheduleReconnect(): void {
    if (this.disposed) return;
    if (this.reconnectTimer) return; // already scheduled

    const delay =
      RECONNECT_DELAYS[
        Math.min(this.reconnectAttempt, RECONNECT_DELAYS.length - 1)
      ];
    this.reconnectAttempt++;
    this.setState("connecting");

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  private cleanup(): void {
    if (this.es) {
      this.es.close();
      this.es = null;
    }
    if (this.keepaliveTimer) {
      clearTimeout(this.keepaliveTimer);
      this.keepaliveTimer = null;
    }
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }
}
