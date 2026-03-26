import type { AgentConfig, PublishOptions } from './types.js';
import { SidecarClient } from './client.js';
import type { BaranEvent } from './events.js';

type EventHandler = (event: BaranEvent) => Promise<Buffer | null | undefined | void>;

/**
 * A Baran OS agent that communicates via the sidecar gateway.
 *
 * @example
 * ```typescript
 * const agent = new BaranAgent({
 *   name: 'echo-agent',
 *   agentType: 'echo',
 *   version: '1.0.0',
 *   token: 'my-secret',
 *   capabilities: [{ name: 'echo.text', version: '1.0.0' }],
 * });
 *
 * agent.on('workflow.step', async (event) => {
 *   return Buffer.from(event.payload['text'] as string ?? '');
 * });
 *
 * await agent.run();
 * ```
 */
export class BaranAgent {
  private readonly config: Required<AgentConfig>;
  private readonly client: SidecarClient;
  private readonly handlers = new Map<string, EventHandler>();
  private _agentId: string | null = null;
  private _running = false;

  constructor(config: AgentConfig) {
    this.config = {
      sidecarUrl: 'http://localhost:9090',
      capabilities: [],
      labels: {},
      agentId: '',
      ...config,
    };
    this.client = new SidecarClient(this.config.sidecarUrl, this.config.token);
  }

  get agentId(): string | null {
    return this._agentId;
  }

  /** Register a handler for a given event type. */
  on(eventType: string, handler: EventHandler): this {
    this.handlers.set(eventType, handler);
    return this;
  }

  /** Register the agent with the sidecar and prepare for events. */
  async start(): Promise<void> {
    const result = await this.client.register(this.config);
    this._agentId = result['agent_id'] as string;
  }

  /** Deregister the agent and close all connections. */
  async stop(): Promise<void> {
    this._running = false;
    if (this._agentId) {
      try {
        await this.client.deregister(this._agentId);
      } catch {
        // best-effort deregistration
      }
      this._agentId = null;
    }
  }

  /** Publish an event through the sidecar. */
  async publish(options: PublishOptions): Promise<Record<string, unknown>> {
    if (!this._agentId) {
      throw new Error('Agent not started — call start() first');
    }
    return this.client.publish({ ...options, agentId: this._agentId });
  }

  /** Start the agent, subscribe to events, and block until process signal. */
  async run(): Promise<void> {
    await this.start();
    this._running = true;

    const abortController = new AbortController();

    const handleSignal = (): void => {
      abortController.abort();
    };

    process.on('SIGINT', handleSignal);
    process.on('SIGTERM', handleSignal);

    try {
      await Promise.race([
        this._eventLoop(abortController.signal),
        new Promise<void>((resolve) => {
          abortController.signal.addEventListener('abort', () => resolve());
        }),
      ]);
    } finally {
      process.off('SIGINT', handleSignal);
      process.off('SIGTERM', handleSignal);
      await this.stop();
    }
  }

  private async _eventLoop(signal: AbortSignal): Promise<void> {
    if (!this._agentId) return;

    for await (const event of this.client.subscribe(this._agentId)) {
      if (!this._running || signal.aborted) break;

      const handler = this.handlers.get(event.eventType);
      if (!handler) continue;

      try {
        const result = await handler(event);
        if (result != null && event.eventId) {
          await this.client.ack(this._agentId!, event.eventId);
        }
      } catch {
        // errors in event handlers are non-fatal
      }
    }
  }
}
