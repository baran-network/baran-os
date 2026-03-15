package discovery

import (
	"context"
	"fmt"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// CapabilityAnnouncer listens to registration lifecycle events and publishes
// agent.capability.announce events to the DISCOVERY stream.
type CapabilityAnnouncer struct {
	bus      eventbus.EventBus
	registry registry.AgentRegistry
	nodeID   string
}

// NewCapabilityAnnouncer creates a new CapabilityAnnouncer.
func NewCapabilityAnnouncer(bus eventbus.EventBus, reg registry.AgentRegistry, nodeID string) *CapabilityAnnouncer {
	return &CapabilityAnnouncer{
		bus:      bus,
		registry: reg,
		nodeID:   nodeID,
	}
}

// Start subscribes to agent.register, agent.unregister, and agent.error events.
func (a *CapabilityAnnouncer) Start(ctx context.Context) ([]eventbus.Subscription, error) {
	regSub, err := a.bus.Subscribe(ctx, "agent.register", a.handleRegister)
	if err != nil {
		return nil, fmt.Errorf("subscribe agent.register: %w", err)
	}

	unregSub, err := a.bus.Subscribe(ctx, "agent.unregister", a.handleUnregister)
	if err != nil {
		_ = regSub.Unsubscribe()
		return nil, fmt.Errorf("subscribe agent.unregister: %w", err)
	}

	errSub, err := a.bus.Subscribe(ctx, "agent.error", a.handleAgentError)
	if err != nil {
		_ = regSub.Unsubscribe()
		_ = unregSub.Unsubscribe()
		return nil, fmt.Errorf("subscribe agent.error: %w", err)
	}

	return []eventbus.Subscription{regSub, unregSub, errSub}, nil
}

func (a *CapabilityAnnouncer) handleRegister(ctx context.Context, evt *eventbus.Event) error {
	var payload protocolv1.AgentRegisterPayload
	if err := proto.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal register payload: %w", err)
	}

	if len(payload.Capabilities) == 0 {
		return nil
	}

	return a.publishAnnounce(ctx, payload.AgentId, payload.Capabilities)
}

func (a *CapabilityAnnouncer) handleUnregister(ctx context.Context, evt *eventbus.Event) error {
	var payload protocolv1.AgentUnregisterPayload
	if err := proto.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal unregister payload: %w", err)
	}

	// Publish announce with empty capabilities to signal removal.
	return a.publishAnnounce(ctx, payload.AgentId, nil)
}

func (a *CapabilityAnnouncer) handleAgentError(ctx context.Context, evt *eventbus.Event) error {
	var payload protocolv1.AgentErrorPayload
	if err := proto.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal error payload: %w", err)
	}

	if payload.ErrorCode != "AGENT_DEAD" {
		return nil
	}

	// Publish deannounce for dead agent.
	return a.publishAnnounce(ctx, payload.AgentId, nil)
}

func (a *CapabilityAnnouncer) publishAnnounce(ctx context.Context, agentID string, caps []*protocolv1.Capability) error {
	announcePayload := &protocolv1.CapabilityAnnouncePayload{
		AgentId:      agentID,
		Capabilities: caps,
	}
	data, err := proto.Marshal(announcePayload)
	if err != nil {
		return fmt.Errorf("marshal announce payload: %w", err)
	}

	return a.bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.capability.announce",
		SourceNode:  a.nodeID,
		SourceAgent: "runtime",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
}
