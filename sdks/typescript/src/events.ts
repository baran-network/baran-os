/** An event received from the Baran OS sidecar gateway. */
export interface BaranEvent {
  eventId: string;
  eventType: string;
  sourceAgent: string;
  sourceNode: string;
  targetAgent: string;
  workflowId?: string;
  correlationId?: string;
  timestamp: number;
  metadata: Record<string, string>;
  payload: Record<string, unknown>;
}

/** Parse a raw SSE data JSON string into a BaranEvent. */
export function parseEvent(eventType: string, eventId: string, data: string): BaranEvent {
  const raw = JSON.parse(data) as Record<string, unknown>;
  return {
    eventId,
    eventType,
    sourceAgent: (raw['source_agent'] as string) ?? '',
    sourceNode: (raw['source_node'] as string) ?? '',
    targetAgent: (raw['target_agent'] as string) ?? '',
    workflowId: raw['workflow_id'] as string | undefined,
    correlationId: raw['correlation_id'] as string | undefined,
    timestamp: (raw['timestamp'] as number) ?? 0,
    metadata: (raw['metadata'] as Record<string, string>) ?? {},
    payload: (raw['payload'] as Record<string, unknown>) ?? {},
  };
}
