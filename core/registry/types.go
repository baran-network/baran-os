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
	Origin           string // "local" or "remote"
	Revision         uint64
}

// IsRemote returns true if this registration originates from a remote node.
func (r AgentRegistration) IsRemote() bool {
	return r.Origin == "remote"
}

// Capability represents a declared ability of an agent.
type Capability struct {
	Name        string
	Version     string
	Description string
	Parameters  map[string]string
}
