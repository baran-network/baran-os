// ---------------------------------------------------------------------------
// Baran OS Operator UI — Shared TypeScript Types
// ---------------------------------------------------------------------------

// -- Status enums -----------------------------------------------------------

export type AgentStatus =
  | "active"
  | "unhealthy"
  | "dead"
  | "unregistered"
  | "unknown";

export type WorkflowStatus =
  | "created"
  | "running"
  | "waiting_human"
  | "completed"
  | "failed";

export type WorkflowStepStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed";

export type ClusterStatus =
  | "active"
  | "unhealthy"
  | "dead"
  | "syncing"
  | "disconnected";

export type ReplayState = "running" | "paused" | "completed" | "error";

export type DecisionAction = "approve" | "reject";

export type ConnectionState = "connected" | "connecting" | "disconnected";

// -- Domain entities --------------------------------------------------------

export interface AgentResources {
  cpu_percent: number;
  memory_bytes: number;
  pending_events: number;
}

export interface Capability {
  name: string;
  version: string;
  description: string | null;
  category: string;
  parameters: Record<string, unknown> | null;
}

export interface Agent {
  id: string;
  name: string;
  type: string;
  version: string;
  status: AgentStatus;
  capabilities: Capability[];
  labels: Record<string, string>;
  origin: "native" | "a2a";
  last_heartbeat: string | null;
  resources: AgentResources | null;
  registered_at: string;
  node_id: string;
}

export interface Event {
  id: string;
  type: string;
  source_agent: string;
  source_node: string;
  target_agent: string | null;
  workflow_id: string | null;
  correlation_id: string | null;
  timestamp: number; // Unix nanoseconds
  metadata: Record<string, string>;
  payload: Record<string, unknown>;
  is_simulated: boolean;
  stream: string;
  sequence: number;
}

export interface WorkflowStep {
  index: number;
  name: string;
  capability: string;
  assigned_agent: string | null;
  status: WorkflowStepStatus;
  result: Record<string, unknown> | null;
  started_at: string | null;
  completed_at: string | null;
  duration_ms: number | null;
}

export interface Workflow {
  id: string;
  name: string;
  status: WorkflowStatus;
  current_step: number;
  total_steps?: number;
  steps?: WorkflowStep[];
  initiator: string;
  created_at: string;
  updated_at: string;
  error: string | null;
}

export interface Cluster {
  id: string;
  name: string;
  address: string;
  status: ClusterStatus;
  agent_count: number;
  workflow_count: number;
  capabilities: string[];
  version: string;
  joined_at: number;
  last_seen: number;
  missed_heartbeats: number;
}

export interface ScenarioStep {
  event_type: string;
  delay_ms: number;
  source_agent: string;
  target_agent: string | null;
  workflow_id: string | null;
  payload_json: Record<string, unknown>;
  metadata: Record<string, string>;
}

export interface Scenario {
  id: string;
  name: string;
  description: string | null;
  /** Number of steps for summary, or full step array for detail. */
  steps: number | ScenarioStep[];
  created_at: string;
}

export interface ScenarioSession {
  id: string;
  scenario_id: string;
  scenario_name: string;
  state: "pending" | "running" | "completed" | "failed" | "stopped";
  current_step: number;
  total_steps: number;
  injected_events: number;
  duration_ms: number;
  error_message: string;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

export interface ReplaySession {
  id: string;
  workflow_id: string;
  state: ReplayState;
  speed: number;
  total_events: number;
  replayed_events: number;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error_message: string | null;
}

export interface Decision {
  decision_id: string;
  workflow_id: string;
  step_index: number;
  step_name: string;
  prompt: string;
  resource_ids: string[];
  conflict_ids: string[];
  requested_at: string;
  /** Set when the decision is resolved (history view). */
  responded_at?: string | null;
  action?: DecisionAction | null;
  operator_id?: string | null;
  comment?: string | null;
}

export interface NetworkStats {
  total_agents: number;
  healthy_count: number;
  degraded_count: number;
  offline_count: number;
  unknown_count: number;
  event_throughput: number;
  active_workflows: number;
  pending_decisions: number;
  federation_nodes: number;
}

// -- API response wrappers --------------------------------------------------

export interface AgentListResponse {
  agents: Agent[];
  total: number;
}

export interface WorkflowListResponse {
  workflows: Workflow[];
  total: number;
  limit: number;
  offset: number;
}

export interface CapabilityEntry {
  name: string;
  category: string;
  description: string;
  agent_count: number;
}

export interface CapabilityListResponse {
  capabilities: CapabilityEntry[];
  total: number;
}

// -- SSE-specific types -----------------------------------------------------

export interface SSEGapEvent {
  skipped: number;
  reason: string;
}

export interface SSEOptions {
  url: string;
  token: string;
  lastEventId?: string;
  onEvent?: (event: Event) => void;
  onGap?: (gap: SSEGapEvent) => void;
  onStateChange?: (state: ConnectionState) => void;
  onError?: (error: globalThis.Error) => void;
  params?: Record<string, string>;
}
