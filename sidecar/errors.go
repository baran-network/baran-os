package sidecar

import "errors"

// Sentinel errors returned by AgentManager operations.
var (
	ErrAgentAlreadyExists = errors.New("agent already registered with this ID")
	ErrAgentNotFound      = errors.New("agent not found")
	ErrMaxAgentsReached   = errors.New("maximum concurrent agent limit reached")
)
