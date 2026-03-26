package sidecar

import (
	"fmt"
	"strings"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"google.golang.org/protobuf/proto"
)

// messageConstructor is a factory function returning a new empty proto.Message.
type messageConstructor func() proto.Message

// PayloadRegistry maps event type strings to proto.Message constructors.
// It is immutable after sidecar startup.
type PayloadRegistry struct {
	entries map[string]messageConstructor
}

// NewPayloadRegistry builds the complete registry from all known protocol payload types.
func NewPayloadRegistry() *PayloadRegistry {
	r := &PayloadRegistry{
		entries: make(map[string]messageConstructor),
	}

	// Agent lifecycle
	r.register("agent.register", func() proto.Message { return &protocolv1.AgentRegisterPayload{} })
	r.register("agent.unregister", func() proto.Message { return &protocolv1.AgentUnregisterPayload{} })
	r.register("agent.error", func() proto.Message { return &protocolv1.AgentErrorPayload{} })

	// Health
	r.register("agent.health.ping", func() proto.Message { return &protocolv1.HealthPingPayload{} })
	r.register("agent.health.pong", func() proto.Message { return &protocolv1.HealthPongPayload{} })

	// Capability / Discovery
	r.register("agent.capability.announce", func() proto.Message { return &protocolv1.CapabilityAnnouncePayload{} })
	r.register("agent.discovery.request", func() proto.Message { return &protocolv1.DiscoveryRequestPayload{} })
	r.register("agent.discovery.response", func() proto.Message { return &protocolv1.DiscoveryResponsePayload{} })

	// Workflow
	r.register("workflow.start", func() proto.Message { return &protocolv1.WorkflowStartPayload{} })
	r.register("workflow.step", func() proto.Message { return &protocolv1.WorkflowStepPayload{} })
	r.register("workflow.step.result", func() proto.Message { return &protocolv1.WorkflowStepResultPayload{} })
	r.register("workflow.complete", func() proto.Message { return &protocolv1.WorkflowCompletePayload{} })
	r.register("workflow.failed", func() proto.Message { return &protocolv1.WorkflowFailedPayload{} })
	r.register("workflow.state.request", func() proto.Message { return &protocolv1.WorkflowStateRequestPayload{} })
	r.register("workflow.state.response", func() proto.Message { return &protocolv1.WorkflowStateResponsePayload{} })

	// Human decisions
	r.register("human.decision.request", func() proto.Message { return &protocolv1.HumanDecisionRequestPayload{} })
	r.register("human.decision.response", func() proto.Message { return &protocolv1.HumanDecisionResponsePayload{} })
	r.register("decision.conflict", func() proto.Message { return &protocolv1.DecisionConflictPayload{} })
	r.register("decision.resolved", func() proto.Message { return &protocolv1.DecisionResolvedPayload{} })

	// Federation
	r.register("federation.node.register", func() proto.Message { return &protocolv1.NodeRegisterPayload{} })
	r.register("federation.node.unregister", func() proto.Message { return &protocolv1.NodeUnregisterPayload{} })
	r.register("federation.node.health.ping", func() proto.Message { return &protocolv1.NodeHealthPingPayload{} })
	r.register("federation.node.health.pong", func() proto.Message { return &protocolv1.NodeHealthPongPayload{} })
	r.register("federation.capability.announce", func() proto.Message { return &protocolv1.FederationCapabilityPayload{} })
	r.register("federation.capability.remove", func() proto.Message { return &protocolv1.FederationCapabilityRemovePayload{} })
	r.register("federation.relay", func() proto.Message { return &protocolv1.FederationRelayPayload{} })

	// Simulation
	r.register("simulation.start", func() proto.Message { return &protocolv1.SimulationStartPayload{} })
	r.register("simulation.stop", func() proto.Message { return &protocolv1.SimulationStopPayload{} })
	r.register("simulation.inject_event", func() proto.Message { return &protocolv1.SimulationInjectEventPayload{} })
	r.register("simulation.replay.start", func() proto.Message { return &protocolv1.SimulationReplayStartPayload{} })
	r.register("simulation.replay.stop", func() proto.Message { return &protocolv1.SimulationReplayStopPayload{} })
	r.register("simulation.replay.complete", func() proto.Message { return &protocolv1.SimulationReplayCompletePayload{} })

	return r
}

func (r *PayloadRegistry) register(eventType string, ctor messageConstructor) {
	r.entries[eventType] = ctor
}

// New returns a new empty proto.Message for the given event type.
// For workflow events with a workflow ID prefix (workflow.<id>.workflow.step.result),
// the prefix is stripped before lookup.
func (r *PayloadRegistry) New(eventType string) (proto.Message, error) {
	key := normalizeEventType(eventType)
	ctor, ok := r.entries[key]
	if !ok {
		return nil, fmt.Errorf("unknown event type %q: no payload schema registered", eventType)
	}
	return ctor(), nil
}

// Has reports whether the registry has a schema for the given event type.
func (r *PayloadRegistry) Has(eventType string) bool {
	_, ok := r.entries[normalizeEventType(eventType)]
	return ok
}

// normalizeEventType strips routing prefixes from event types so the registry
// can locate the correct payload schema.
//
// Handled prefixes:
//   - "workflow.<id>."  → "workflow.<id>.workflow.step.result" → "workflow.step.result"
//   - "agent.direct.<agentID>." → "agent.direct.x.agent.health.ping" → "agent.health.ping"
func normalizeEventType(eventType string) string {
	if strings.HasPrefix(eventType, "workflow.") {
		// Strip "workflow.<id>." prefix: "workflow.abc123.workflow.step.result"
		rest := eventType[len("workflow."):]
		dot := strings.Index(rest, ".")
		if dot < 0 {
			return eventType
		}
		suffix := rest[dot+1:]
		// Only strip if the suffix itself starts with "workflow."
		if strings.HasPrefix(suffix, "workflow.") {
			return suffix
		}
		return eventType
	}
	// Strip "agent.direct.<agentID>." prefix for direct-delivery events.
	// e.g. "agent.direct.my-agent.agent.health.ping" → "agent.health.ping"
	if strings.HasPrefix(eventType, "agent.direct.") {
		rest := eventType[len("agent.direct."):]
		dot := strings.Index(rest, ".")
		if dot >= 0 {
			return rest[dot+1:]
		}
	}
	return eventType
}
