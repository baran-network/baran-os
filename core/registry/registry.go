package registry

import "context"

// AgentRegistry manages agent registration state with optimistic concurrency.
type AgentRegistry interface {
	// Register creates or re-registers an agent. Returns the new KV revision.
	Register(ctx context.Context, reg AgentRegistration) (uint64, error)

	// Deregister removes an agent from the registry. Idempotent.
	Deregister(ctx context.Context, agentID string) error

	// Get retrieves an agent registration by ID along with its current revision.
	Get(ctx context.Context, agentID string) (AgentRegistration, uint64, error)

	// List returns all currently registered agents.
	List(ctx context.Context) ([]AgentRegistration, error)

	// UpdateStatus updates an agent's lifecycle status with CAS.
	UpdateStatus(ctx context.Context, agentID string, status AgentLifecycleStatus, revision uint64) (uint64, error)

	// RecordHeartbeat records a successful heartbeat, resets missed counter,
	// and transitions UNHEALTHY→ACTIVE if needed.
	RecordHeartbeat(ctx context.Context, agentID string, revision uint64) (uint64, error)

	// IncrementMissedHeartbeats increments the missed counter and transitions
	// at thresholds (3→UNHEALTHY, 6→DEAD).
	IncrementMissedHeartbeats(ctx context.Context, agentID string, revision uint64) (AgentLifecycleStatus, uint64, error)
}
