package discovery

import (
	"context"
	"fmt"
	"time"

	"github.com/ad-hok/agent-os/core/eventbus"
	"github.com/ad-hok/agent-os/core/registry"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/ad-hok/agent-os/protocol/gen/go/agentosprotocol/v1"
)

// DiscoveryHandler subscribes to agent.discovery.request events and publishes
// agent.discovery.response events with matching agents.
type DiscoveryHandler struct {
	bus      eventbus.EventBus
	registry registry.AgentRegistry
	nodeID   string
}

// NewDiscoveryHandler creates a new DiscoveryHandler.
func NewDiscoveryHandler(bus eventbus.EventBus, reg registry.AgentRegistry, nodeID string) *DiscoveryHandler {
	return &DiscoveryHandler{
		bus:      bus,
		registry: reg,
		nodeID:   nodeID,
	}
}

// Start subscribes to agent.discovery.request events.
func (h *DiscoveryHandler) Start(ctx context.Context) ([]eventbus.Subscription, error) {
	sub, err := h.bus.Subscribe(ctx, "agent.discovery.request", h.handleDiscoveryRequest)
	if err != nil {
		return nil, fmt.Errorf("subscribe agent.discovery.request: %w", err)
	}
	return []eventbus.Subscription{sub}, nil
}

func (h *DiscoveryHandler) handleDiscoveryRequest(ctx context.Context, evt *eventbus.Event) error {
	var payload protocolv1.DiscoveryRequestPayload
	if err := proto.Unmarshal(evt.Payload, &payload); err != nil {
		return h.publishError(ctx, evt.SourceAgent, "INVALID_PAYLOAD",
			fmt.Sprintf("failed to unmarshal discovery request: %v", err))
	}

	if payload.CapabilityName == "" {
		return h.publishError(ctx, evt.SourceAgent, "INVALID_DISCOVERY_REQUEST",
			"capability_name is required")
	}

	agents, err := h.registry.FindByCapability(ctx, payload.CapabilityName, payload.VersionConstraint)
	if err != nil {
		return h.publishError(ctx, evt.SourceAgent, "DISCOVERY_FAILED",
			fmt.Sprintf("registry query failed: %v", err))
	}

	matches := make([]*protocolv1.AgentCapabilityMatch, len(agents))
	for i, agent := range agents {
		caps := make([]*protocolv1.Capability, len(agent.Capabilities))
		for j, c := range agent.Capabilities {
			caps[j] = &protocolv1.Capability{
				Name:        c.Name,
				Version:     c.Version,
				Description: c.Description,
				Parameters:  c.Parameters,
			}
		}
		matches[i] = &protocolv1.AgentCapabilityMatch{
			AgentId:      agent.AgentID,
			AgentType:    agent.AgentType,
			Capabilities: caps,
		}
	}

	responsePayload := &protocolv1.DiscoveryResponsePayload{
		Matches: matches,
	}
	data, err := proto.Marshal(responsePayload)
	if err != nil {
		return fmt.Errorf("marshal discovery response: %w", err)
	}

	return h.bus.Publish(ctx, &eventbus.Event{
		ID:            uuid.Must(uuid.NewV7()).String(),
		Type:          "agent.discovery.response",
		SourceNode:    h.nodeID,
		SourceAgent:   "runtime",
		CorrelationID: evt.CorrelationID,
		Timestamp:     time.Now().UnixNano(),
		Payload:       data,
	})
}

func (h *DiscoveryHandler) publishError(ctx context.Context, agentID, code, message string) error {
	errPayload := &protocolv1.AgentErrorPayload{
		AgentId:   agentID,
		ErrorCode: code,
		Message:   message,
	}
	data, err := proto.Marshal(errPayload)
	if err != nil {
		return fmt.Errorf("marshal error payload: %w", err)
	}

	return h.bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.error",
		SourceNode:  h.nodeID,
		SourceAgent: "runtime",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
}
