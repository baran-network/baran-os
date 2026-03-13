package registry

import "fmt"

// AgentLifecycleStatus represents the lifecycle state of an agent in the runtime.
type AgentLifecycleStatus int

const (
	StatusRegistering AgentLifecycleStatus = iota
	StatusActive
	StatusUnhealthy
	StatusDead
	StatusUnregistered
)

func (s AgentLifecycleStatus) String() string {
	switch s {
	case StatusRegistering:
		return "REGISTERING"
	case StatusActive:
		return "ACTIVE"
	case StatusUnhealthy:
		return "UNHEALTHY"
	case StatusDead:
		return "DEAD"
	case StatusUnregistered:
		return "UNREGISTERED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}
