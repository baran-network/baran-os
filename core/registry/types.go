package registry

import "time"

// AgentRegistration holds the state of a registered agent in the runtime.
type AgentRegistration struct {
	AgentID          string
	AgentType        string
	Version          string
	Capabilities     []Capability
	Labels           map[string]string
	Status           AgentLifecycleStatus
	LastSeen         time.Time
	MissedHeartbeats int32
	NodeID           string
	Origin           string // "local", "remote", or "a2a"
	Revision         uint64
}

// IsRemote returns true if this registration originates from a remote node.
func (r AgentRegistration) IsRemote() bool {
	return r.Origin == "remote"
}

// IsA2A returns true if this registration represents an external A2A agent.
func (r AgentRegistration) IsA2A() bool {
	return r.Origin == "a2a"
}

// Capability represents a declared ability of an agent.
type Capability struct {
	Name        string
	Version     string
	Description string
	Parameters  map[string]string
	// Taxonomy fields (Phase 9). Auto-populated for standard capabilities.
	Category    string
	Action      string
	InputTypes  []string
	OutputTypes []string
}
