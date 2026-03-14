package router

import "github.com/carlosmolina/agent-os/core/eventbus"

// RoutingStrategy classifies how an event should be routed.
type RoutingStrategy int

const (
	// StrategyBroadcast delivers to all subscribers of the event type (default).
	StrategyBroadcast RoutingStrategy = iota
	// StrategyDirect delivers exclusively to a named target agent.
	StrategyDirect
	// StrategyWorkflow delivers to a per-workflow stream.
	StrategyWorkflow
	// StrategyCapability fans out to agents offering a specific capability.
	StrategyCapability
)

// String returns a human-readable name for the strategy.
func (s RoutingStrategy) String() string {
	switch s {
	case StrategyBroadcast:
		return "broadcast"
	case StrategyDirect:
		return "direct"
	case StrategyWorkflow:
		return "workflow"
	case StrategyCapability:
		return "capability"
	default:
		return "unknown"
	}
}

// RoutingDecision is the resolved outcome for a given event.
type RoutingDecision struct {
	Strategy RoutingStrategy
	Stream   string
	Subject  string
}

// ResolveStrategy inspects envelope fields and returns the correct RoutingStrategy.
// Precedence: Direct > Capability > Workflow > Broadcast.
func ResolveStrategy(event *eventbus.Event) RoutingStrategy {
	if event.TargetAgent != "" {
		return StrategyDirect
	}
	if event.Metadata != nil {
		if _, ok := event.Metadata["route.capability"]; ok {
			return StrategyCapability
		}
	}
	if event.WorkflowID != "" {
		return StrategyWorkflow
	}
	return StrategyBroadcast
}
