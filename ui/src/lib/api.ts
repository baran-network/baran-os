// ---------------------------------------------------------------------------
// Baran OS Operator UI — Runtime API Client
// ---------------------------------------------------------------------------

import type {
  Agent,
  AgentListResponse,
  CapabilityListResponse,
  Decision,
  NetworkStats,
  ReplaySession,
  Scenario,
  ScenarioSession,
  Workflow,
  WorkflowListResponse,
} from "./types";

// -- Configuration ----------------------------------------------------------

function getRuntimeUrl(): string {
  const url =
    process.env.NEXT_PUBLIC_BARAN_RUNTIME_URL ?? "http://localhost:8080";
  return url.replace(/\/+$/, "");
}

function getToken(): string {
  return process.env.BARAN_UI_TOKEN ?? "";
}

// -- Fetch wrapper ----------------------------------------------------------

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function request<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  const url = `${getRuntimeUrl()}${path}`;
  const token = getToken();

  const res = await fetch(url, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...init?.headers,
    },
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new ApiError(res.status, body.error ?? res.statusText);
  }

  return res.json() as Promise<T>;
}

// -- Query helpers ----------------------------------------------------------

function qs(params: object): string {
  const entries = Object.entries(params).filter(
    (e): e is [string, string | number] => e[1] !== undefined && e[1] !== null,
  );
  if (entries.length === 0) return "";
  return "?" + new URLSearchParams(entries.map(([k, v]) => [k, String(v)])).toString();
}

// -- Agent endpoints --------------------------------------------------------

export interface AgentQuery {
  status?: string;
  capability?: string;
  type?: string;
  q?: string;
}

export async function fetchAgents(
  query?: AgentQuery,
): Promise<AgentListResponse> {
  return request<AgentListResponse>(`/api/agents${qs(query ?? {})}`);
}

export async function fetchAgent(agentId: string): Promise<Agent> {
  return request<Agent>(`/api/agents/${encodeURIComponent(agentId)}`);
}

// -- Workflow endpoints -----------------------------------------------------

export interface WorkflowQuery {
  status?: string;
  limit?: number;
  offset?: number;
}

export async function fetchWorkflows(
  query?: WorkflowQuery,
): Promise<WorkflowListResponse> {
  return request<WorkflowListResponse>(`/api/workflows${qs(query ?? {})}`);
}

export async function fetchWorkflow(workflowId: string): Promise<Workflow> {
  return request<Workflow>(
    `/api/workflows/${encodeURIComponent(workflowId)}`,
  );
}

// -- Capability endpoints ---------------------------------------------------

export async function fetchCapabilities(): Promise<CapabilityListResponse> {
  return request<CapabilityListResponse>("/api/capabilities");
}

// -- Stats endpoint ---------------------------------------------------------

export async function fetchStats(): Promise<NetworkStats> {
  return request<NetworkStats>("/api/stats");
}

// -- Decision endpoints -----------------------------------------------------

export async function fetchDecisions(): Promise<Decision[]> {
  const res = await request<{ decisions: Decision[] }>("/api/decisions");
  return res.decisions ?? [];
}

export async function fetchDecision(decisionId: string): Promise<Decision> {
  return request<Decision>(
    `/api/decisions/${encodeURIComponent(decisionId)}`,
  );
}

export async function respondDecision(
  decisionId: string,
  action: "approve" | "reject",
  operatorId: string,
  comment?: string,
): Promise<void> {
  await request<unknown>(
    `/api/decisions/${encodeURIComponent(decisionId)}/respond`,
    {
      method: "POST",
      body: JSON.stringify({ action, operator_id: operatorId, comment }),
    },
  );
}

// -- Federation endpoints ---------------------------------------------------

export async function fetchFederationNodes(): Promise<Cluster[]> {
  return request<Cluster[]>("/api/federation/nodes");
}

// Reuse the Cluster type from types.ts
import type { Cluster } from "./types";

// -- Replay endpoints -------------------------------------------------------

export async function createReplaySession(
  workflowId: string,
  speed: number,
): Promise<ReplaySession> {
  const res = await request<{ session: ReplaySession }>(
    "/api/replay/sessions",
    {
      method: "POST",
      body: JSON.stringify({ workflow_id: workflowId, speed }),
    },
  );
  return res.session;
}

export async function fetchReplaySessions(): Promise<ReplaySession[]> {
  const res = await request<{ sessions: ReplaySession[] }>(
    "/api/replay/sessions",
  );
  return res.sessions ?? [];
}

export async function fetchReplaySession(
  sessionId: string,
): Promise<ReplaySession> {
  return request<ReplaySession>(
    `/api/replay/sessions/${encodeURIComponent(sessionId)}`,
  );
}

export async function stopReplaySession(sessionId: string): Promise<void> {
  await request<unknown>(
    `/api/replay/sessions/${encodeURIComponent(sessionId)}/stop`,
    { method: "POST" },
  );
}

// -- Simulation endpoints ---------------------------------------------------

export async function fetchScenarios(): Promise<Scenario[]> {
  const res = await request<{ scenarios: Scenario[] }>(
    "/api/simulation/scenarios",
  );
  return res.scenarios ?? [];
}

export async function fetchScenario(scenarioId: string): Promise<Scenario> {
  const res = await request<{ scenario: Scenario }>(
    `/api/simulation/scenarios/${encodeURIComponent(scenarioId)}`,
  );
  return res.scenario;
}

export async function startScenario(
  scenarioId: string,
): Promise<ScenarioSession> {
  const res = await request<{ session: ScenarioSession }>(
    `/api/simulation/scenarios/${encodeURIComponent(scenarioId)}/start`,
    { method: "POST" },
  );
  return res.session;
}

export async function fetchScenarioSession(
  sessionId: string,
): Promise<ScenarioSession> {
  const res = await request<{ session: ScenarioSession }>(
    `/api/simulation/sessions/${encodeURIComponent(sessionId)}`,
  );
  return res.session;
}

export async function injectEvent(payload: {
  type: string;
  source_agent: string;
  target_agent?: string;
  workflow_id?: string;
  payload: Record<string, unknown>;
  metadata?: Record<string, string>;
}): Promise<void> {
  await request<unknown>("/api/simulation/inject", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

// -- Event query endpoints --------------------------------------------------

export async function fetchWorkflowEvents(
  workflowId: string,
): Promise<Event[]> {
  return request<Event[]>(
    `/api/events/workflows/${encodeURIComponent(workflowId)}`,
  );
}

import type { Event } from "./types";

// -- SSE URL builder --------------------------------------------------------

export function buildSSEUrl(
  path: string,
  params?: Record<string, string>,
): string {
  const base = getRuntimeUrl();
  return `${base}${path}${qs(params ?? {})}`;
}

export function getAuthToken(): string {
  return getToken();
}
