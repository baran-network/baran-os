import type { AgentConfig, Capability, PublishOptions } from './types.js';
import {
  BaranAuthError,
  BaranConnectionError,
  BaranPublishError,
  BaranRegistrationError,
} from './errors.js';
import { BaranEvent, parseEvent } from './events.js';

function buildRegisterBody(config: AgentConfig): Record<string, unknown> {
  const body: Record<string, unknown> = {
    name: config.name,
    agent_type: config.agentType,
    version: config.version,
  };
  if (config.agentId) body['agent_id'] = config.agentId;
  if (config.capabilities && config.capabilities.length > 0) {
    body['capabilities'] = config.capabilities.map((c: Capability) => {
      const cap: Record<string, unknown> = {
        name: c.name,
        version: c.version,
        description: c.description ?? '',
      };
      if (c.parameters) cap['parameters'] = c.parameters;
      return cap;
    });
  }
  if (config.labels && Object.keys(config.labels).length > 0) {
    body['labels'] = config.labels;
  }
  return body;
}

async function raiseForStatus(resp: Response): Promise<void> {
  if (resp.status === 401) {
    const text = await resp.text();
    throw new BaranAuthError(`Authentication failed: ${text}`);
  }
  if (resp.status >= 400) {
    let msg: string;
    try {
      const body = (await resp.json()) as Record<string, unknown>;
      msg = (body['error'] as string) ?? resp.statusText;
    } catch {
      msg = await resp.text();
    }
    if (resp.status === 409) throw new BaranRegistrationError(`Agent conflict: ${msg}`);
    if (resp.status === 503) throw new BaranRegistrationError(`Service unavailable: ${msg}`);
    throw new BaranConnectionError(`HTTP ${resp.status}: ${msg}`);
  }
}

/** Low-level HTTP client for the Baran OS sidecar gateway. */
export class SidecarClient {
  readonly baseUrl: string;
  private readonly token: string;

  constructor(baseUrl: string, token: string) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.token = token;
  }

  private get authHeaders(): Record<string, string> {
    return {
      Authorization: `Bearer ${this.token}`,
      'Content-Type': 'application/json',
    };
  }

  async register(config: AgentConfig): Promise<Record<string, unknown>> {
    let resp: Response;
    try {
      resp = await fetch(`${this.baseUrl}/agents`, {
        method: 'POST',
        headers: this.authHeaders,
        body: JSON.stringify(buildRegisterBody(config)),
      });
    } catch (err) {
      throw new BaranConnectionError(
        `Cannot reach sidecar at ${this.baseUrl}: ${err}`,
      );
    }
    await raiseForStatus(resp);
    return resp.json() as Promise<Record<string, unknown>>;
  }

  async deregister(agentId: string): Promise<Record<string, unknown>> {
    let resp: Response;
    try {
      resp = await fetch(`${this.baseUrl}/agents/${agentId}`, {
        method: 'DELETE',
        headers: this.authHeaders,
      });
    } catch (err) {
      throw new BaranConnectionError(`Cannot reach sidecar: ${err}`);
    }
    await raiseForStatus(resp);
    return resp.json() as Promise<Record<string, unknown>>;
  }

  async publish(
    options: PublishOptions & { agentId: string },
  ): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = {
      type: options.eventType,
      payload: options.payload,
    };
    if (options.targetAgent) body['target_agent'] = options.targetAgent;
    if (options.workflowId) body['workflow_id'] = options.workflowId;
    if (options.correlationId) body['correlation_id'] = options.correlationId;
    if (options.metadata) body['metadata'] = options.metadata;

    let resp: Response;
    try {
      resp = await fetch(`${this.baseUrl}/agents/${options.agentId}/events`, {
        method: 'POST',
        headers: this.authHeaders,
        body: JSON.stringify(body),
      });
    } catch (err) {
      throw new BaranConnectionError(`Cannot reach sidecar: ${err}`);
    }
    if (resp.status >= 400) {
      const text = await resp.text();
      throw new BaranPublishError(`Publish failed (${resp.status}): ${text}`);
    }
    return resp.json() as Promise<Record<string, unknown>>;
  }

  async ack(agentId: string, eventId: string): Promise<void> {
    let resp: Response;
    try {
      resp = await fetch(`${this.baseUrl}/agents/${agentId}/ack`, {
        method: 'POST',
        headers: this.authHeaders,
        body: JSON.stringify({ event_id: eventId }),
      });
    } catch (err) {
      throw new BaranConnectionError(`Cannot reach sidecar: ${err}`);
    }
    await raiseForStatus(resp);
  }

  /**
   * Subscribe to events via SSE. Yields BaranEvent objects.
   * Uses fetch + ReadableStream to handle all event types generically.
   */
  async *subscribe(
    agentId: string,
    lastEventId?: string,
  ): AsyncGenerator<BaranEvent> {
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.token}`,
      Accept: 'text/event-stream',
    };
    if (lastEventId) headers['Last-Event-ID'] = lastEventId;

    let resp: Response;
    try {
      resp = await fetch(`${this.baseUrl}/agents/${agentId}/events`, { headers });
    } catch (err) {
      throw new BaranConnectionError(`Cannot reach sidecar: ${err}`);
    }

    if (resp.status === 401) {
      const text = await resp.text();
      throw new BaranAuthError(`Authentication failed: ${text}`);
    }
    if (!resp.ok) {
      const text = await resp.text();
      throw new BaranConnectionError(`HTTP ${resp.status}: ${text}`);
    }
    if (!resp.body) return;

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let currentType = '';
    let currentId = '';
    let currentData = '';

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';

        for (const line of lines) {
          if (line === '' || line === '\r') {
            // Blank line dispatches the event
            if (currentType && currentData) {
              try {
                yield parseEvent(currentType, currentId, currentData);
              } catch {
                // skip unparseable events
              }
            }
            currentType = '';
            currentId = '';
            currentData = '';
          } else if (line.startsWith('event:')) {
            currentType = line.slice(6).trim();
          } else if (line.startsWith('id:')) {
            currentId = line.slice(3).trim();
          } else if (line.startsWith('data:')) {
            currentData = line.slice(5).trim();
          }
          // Lines starting with ':' are SSE comments — skip silently
        }
      }
    } finally {
      reader.cancel().catch(() => undefined);
    }
  }
}
