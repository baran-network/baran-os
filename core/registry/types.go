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
	Revision         uint64
}

// Capability represents a declared ability of an agent.
type Capability struct {
	Name        string
	Version     string
	Description string
	Parameters  map[string]string
}
