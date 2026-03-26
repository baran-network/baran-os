/**
 * Integration tests for the Baran TypeScript SDK.
 *
 * These tests require a live sidecar gateway. Start one with:
 *   baran-sidecar --nats-url nats://localhost:4222 --psk test-secret
 *
 * Set BARAN_SIDECAR_URL to override the default (http://localhost:9090).
 * Set BARAN_SIDECAR_PSK to override the default PSK (test-secret).
 */

import { BaranAgent } from '../src/agent.js';
import { SidecarClient } from '../src/client.js';
import { BaranAuthError } from '../src/errors.js';
import type { AgentConfig } from '../src/types.js';

const SIDECAR_URL = process.env['BARAN_SIDECAR_URL'] ?? 'http://localhost:9090';
const PSK = process.env['BARAN_SIDECAR_PSK'] ?? 'test-secret';

async function sidecarReachable(): Promise<boolean> {
  try {
    const resp = await fetch(`${SIDECAR_URL}/health`, { signal: AbortSignal.timeout(2000) });
    return resp.status === 200 || resp.status === 503;
  } catch {
    return false;
  }
}

// Skip all tests if sidecar is not reachable
beforeAll(async () => {
  if (!(await sidecarReachable())) {
    console.warn(`Sidecar not reachable at ${SIDECAR_URL} — skipping integration tests`);
  }
});

function skipIfNoSidecar(): void {
  // Individual tests call this and use conditional skip via jest.retryTimes or similar
}

const runIf = (condition: boolean) => (condition ? describe : describe.skip);

describe('SidecarClient', () => {
  let canReach = false;

  beforeAll(async () => {
    canReach = await sidecarReachable();
  });

  it('registers and deregisters an agent', async () => {
    if (!canReach) return;

    const client = new SidecarClient(SIDECAR_URL, PSK);
    const config: AgentConfig = {
      name: 'ts-test-client-agent',
      agentType: 'tester',
      version: '1.0.0',
      token: PSK,
      capabilities: [{ name: 'test.echo', version: '1.0.0' }],
    };

    const result = await client.register(config);
    expect(result['agent_id']).toBeDefined();
    expect(result['status']).toBe('active');

    const agentId = result['agent_id'] as string;
    const dereg = await client.deregister(agentId);
    expect(dereg['status']).toBe('deregistered');
  });

  it('throws BaranAuthError on invalid PSK', async () => {
    if (!canReach) return;

    const client = new SidecarClient(SIDECAR_URL, 'wrong-token');
    const config: AgentConfig = {
      name: 'bad-auth-agent',
      agentType: 'tester',
      version: '1.0.0',
      token: 'wrong-token',
    };

    await expect(client.register(config)).rejects.toThrow(BaranAuthError);
  });

  it('publishes an event', async () => {
    if (!canReach) return;

    const client = new SidecarClient(SIDECAR_URL, PSK);
    const config: AgentConfig = {
      name: 'ts-publisher-agent',
      agentType: 'tester',
      version: '1.0.0',
      token: PSK,
      capabilities: [{ name: 'test.publish', version: '1.0.0' }],
    };

    const reg = await client.register(config);
    const agentId = reg['agent_id'] as string;

    try {
      const result = await client.publish({
        agentId,
        eventType: 'agent.health.ping',
        payload: {},
      });
      expect(result['event_id']).toBeDefined();
    } finally {
      await client.deregister(agentId);
    }
  });
});

describe('BaranAgent', () => {
  let canReach = false;

  beforeAll(async () => {
    canReach = await sidecarReachable();
  });

  it('starts and stops (lifecycle)', async () => {
    if (!canReach) return;

    const agent = new BaranAgent({
      name: 'ts-lifecycle-agent',
      agentType: 'tester',
      version: '1.0.0',
      token: PSK,
      sidecarUrl: SIDECAR_URL,
      capabilities: [{ name: 'test.lifecycle', version: '1.0.0' }],
    });

    await agent.start();
    expect(agent.agentId).toBeDefined();
    await agent.stop();
    expect(agent.agentId).toBeNull();
  });

  it('publishes events via agent.publish()', async () => {
    if (!canReach) return;

    const agent = new BaranAgent({
      name: 'ts-pub-agent',
      agentType: 'tester',
      version: '1.0.0',
      token: PSK,
      sidecarUrl: SIDECAR_URL,
      capabilities: [{ name: 'test.pub', version: '1.0.0' }],
    });

    await agent.start();
    try {
      const result = await agent.publish({
        eventType: 'agent.health.ping',
        payload: {},
      });
      expect(result['event_id']).toBeDefined();
    } finally {
      await agent.stop();
    }
  });

  it('registers event handlers via on()', async () => {
    if (!canReach) return;

    const agent = new BaranAgent({
      name: 'ts-handler-agent',
      agentType: 'tester',
      version: '1.0.0',
      token: PSK,
      sidecarUrl: SIDECAR_URL,
    });

    const received: string[] = [];

    agent.on('workflow.step', async (event) => {
      received.push(event.eventType);
      return Buffer.from('ok');
    });

    // Verify handler is registered (internal state check)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((agent as any).handlers.has('workflow.step')).toBe(true);

    await agent.start();
    expect(agent.agentId).toBeDefined();
    await agent.stop();
  });
});
