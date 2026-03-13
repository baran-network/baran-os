package registry

import (
	"context"
	"fmt"
	"time"

	"github.com/carlosmolina/agent-os/core/eventbus"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/carlosmolina/agent-os/protocol/gen/go/agentosprotocol/v1"
)

// Handler subscribes to registration and unregistration events via the EventBus
// and delegates to the AgentRegistry.
type Handler struct {
	bus      eventbus.EventBus
	registry AgentRegistry
	nodeID   string
}

// NewHandler creates a Handler that bridges events to the registry.
func NewHandler(bus eventbus.EventBus, registry AgentRegistry, nodeID string) *Handler {
	return &Handler{bus: bus, registry: registry, nodeID: nodeID}
}

// Start subscribes to agent.register and agent.unregister events.
func (h *Handler) Start(ctx context.Context) ([]eventbus.Subscription, error) {
	regSub, err := h.bus.Subscribe(ctx, "agent.register", h.handleRegister)
	if err != nil {
		return nil, fmt.Errorf("subscribe agent.register: %w", err)
	}

	unregSub, err := h.bus.Subscribe(ctx, "agent.unregister", h.handleUnregister)
	if err != nil {
		_ = regSub.Unsubscribe()
		return nil, fmt.Errorf("subscribe agent.unregister: %w", err)
	}

	return []eventbus.Subscription{regSub, unregSub}, nil
}

func (h *Handler) handleRegister(ctx context.Context, evt *eventbus.Event) error {
	var payload protocolv1.AgentRegisterPayload
	if err := proto.Unmarshal(evt.Payload, &payload); err != nil {
		return h.publishError(ctx, evt.SourceAgent, "INVALID_PAYLOAD", fmt.Sprintf("failed to unmarshal register payload: %v", err))
	}

	caps := make([]Capability, len(payload.Capabilities))
	for i, c := range payload.Capabilities {
		caps[i] = Capability{
			Name:        c.Name,
			Version:     c.Version,
			Description: c.Description,
			Parameters:  c.Parameters,
		}
	}

	reg := AgentRegistration{
		AgentID:      payload.AgentId,
		AgentType:    payload.AgentType,
		Version:      payload.Version,
		Capabilities: caps,
		Labels:       payload.Labels,
		NodeID:       h.nodeID,
	}

	_, err := h.registry.Register(ctx, reg)
	if err != nil {
		return h.publishError(ctx, payload.AgentId, "REGISTRATION_FAILED", err.Error())
	}

	return nil
}

func (h *Handler) handleUnregister(ctx context.Context, evt *eventbus.Event) error {
	var payload protocolv1.AgentUnregisterPayload
	if err := proto.Unmarshal(evt.Payload, &payload); err != nil {
		return h.publishError(ctx, evt.SourceAgent, "INVALID_PAYLOAD", fmt.Sprintf("failed to unmarshal unregister payload: %v", err))
	}

	return h.registry.Deregister(ctx, payload.AgentId)
}

func (h *Handler) publishError(ctx context.Context, agentID, code, message string) error {
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
